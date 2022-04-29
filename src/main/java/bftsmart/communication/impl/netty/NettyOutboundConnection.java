package bftsmart.communication.impl.netty;

import bftsmart.communication.DHPubKeyCertificate;
import bftsmart.communication.MacKey;
import bftsmart.communication.MacKeyGenerator;
import bftsmart.communication.MessageQueue;
import bftsmart.reconfiguration.ViewTopology;
import bftsmart.util.SSLContextFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.io.BytesUtils;
import utils.net.SSLSecurity;

import javax.net.ssl.SSLEngine;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * Netty 对外连接
 */
public class NettyOutboundConnection extends AbstractNettyConnection {

    // 尝试重连时间间隔，单位：秒
    private static final int RECONNECT_TRYING_INTERVAL = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyOutboundConnection.class);

    private NettyClient communicationClient;
    private MacKeyGenerator macKeyGen;
    private SSLSecurity sslSecurity;

    public NettyOutboundConnection(String realmName, ViewTopology viewTopology, int remoteId, MessageQueue messageInQueue, SSLSecurity sslSecurity) {
        super(realmName, viewTopology, remoteId, messageInQueue);
        this.sslSecurity = sslSecurity;
        this.communicationClient = new NettyClient(viewTopology.getStaticConf().getHost(REMOTE_ID),
                viewTopology.getStaticConf().getServerToServerPort(REMOTE_ID), viewTopology.getStaticConf().isSecure(REMOTE_ID));
        this.macKeyGen = new MacKeyGenerator(viewTopology.getStaticConf().getRSAPublicKey(),
                viewTopology.getStaticConf().getRSAPrivateKey(), viewTopology.getStaticConf().getDHG(),
                viewTopology.getStaticConf().getDHP());
    }

    @Override
    public void start() {
        super.start();
        communicationClient.start();
    }

    @Override
    public void close() {
        super.close();
        communicationClient.close();
    }

    /**
     * Netty客户端连接
     */
    public class NettyClient implements NettyChannelHandler, Closeable {

        private String host;
        private int port;
        private boolean secure;
        private Bootstrap bootstrap;
        private NioEventLoopGroup workerGroup;
        private ChannelFuture future;
        private volatile boolean authorized = false;

        public NettyClient(String host, int port, boolean secure) {
            this.host = host;
            this.port = port;
            this.secure = secure;

            workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);

            InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
            workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());
            bootstrap = new Bootstrap().group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new NettyInitializerHandler(this));
        }

        /**
         * 启动/重新连接
         */
        protected void start() {
            // 掉线重试时先关闭原来通道
            if (null != future) {
                future.channel().closeFuture();
            }
            future = connect().addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    // 连接不成功，重试连接
                    future.channel().eventLoop().schedule(this::start, RECONNECT_TRYING_INTERVAL, TimeUnit.SECONDS);
                }
            });
        }

        private ChannelFuture connect() {
            LOGGER.info("Try to connect netty server {}:{}", host, port);
            return bootstrap.connect(host, port);
        }

        @Override
        public void close() {
            try {
                if (future != null) {
                    future.channel().close().syncUninterruptibly();
                    future = null;
                }
            } finally {
                if (workerGroup != null) {
                    workerGroup.shutdownGracefully().syncUninterruptibly();
                    workerGroup = null;
                }
            }
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception {
            if(secure) {
                SSLEngine sslEngine = SSLContextFactory.getSSLContext(true, sslSecurity).createSSLEngine();
                sslEngine.setUseClientMode(true);
                if(null != sslSecurity.getEnabledProtocols() && sslSecurity.getEnabledProtocols().length > 0) {
                    sslEngine.setEnabledProtocols(sslSecurity.getEnabledProtocols());
                }
                if(null != sslSecurity.getCiphers() && sslSecurity.getCiphers().length > 0) {
                    sslEngine.setEnabledCipherSuites(sslSecurity.getCiphers());
                }

                channel.pipeline().addFirst(new SslHandler(sslEngine))
                        .addLast(new LengthFieldBasedFrameDecoder(MAX_MESSAGE_SIZE, 0, 4, 0, 4))
                        .addLast("msg decoder", new BytesDecoder())
                        .addLast(new LengthFieldPrepender(4, 0, false))
                        .addLast("msg encoder", new BytesEncoder())
                        .addLast(new NettyInboundHandlerAdapter(this));
            } else {
                channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(MAX_MESSAGE_SIZE, 0, 4, 0, 4))
                        .addLast("msg decoder", new BytesDecoder())
                        .addLast(new LengthFieldPrepender(4, 0, false))
                        .addLast("msg encoder", new BytesEncoder())
                        .addLast(new NettyInboundHandlerAdapter(this));
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            LOGGER.debug("Outbound channel connected. me:{}, remote:{}", ME, ctx.channel().remoteAddress());

            // 发送ID
            byte[] idBytes = BytesUtils.toBytes(ME);
            // 发送MAC密钥交换信息，发送 DH key
            byte[] macBytes = macKeyGen.getDHPubKeyCertificate().getEncodedBytes();
            byte[] bytes = new byte[idBytes.length + macBytes.length];
            System.arraycopy(idBytes, 0, bytes, 0, idBytes.length);
            System.arraycopy(macBytes, 0, bytes, idBytes.length, macBytes.length);
            ctx.writeAndFlush(bytes);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, byte[] msg) {
            if (!authorized) {
                if (!authorize(ctx, msg)) {
                    receiveMessage(msg);
                }
            } else {
                receiveMessage(msg);
            }
        }

        @Override
        public synchronized void channelInactive(ChannelHandlerContext ctx) {
            LOGGER.debug("Outbound channel disconnected. me:{}, remote:{}", ME, ctx.channel().remoteAddress());
            if (context != null) {
                context = null;
                if (future != null) {
                    // 掉线重连，重试连接
                    future.channel().eventLoop().schedule(this::start, RECONNECT_TRYING_INTERVAL, TimeUnit.SECONDS);
                }
            }
        }

        @Override
        public synchronized void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOGGER.error("Outbound channel exceptionCaught. me:{}, remote:{}", ME, ctx.channel().remoteAddress(), cause);
            if (context != null) {
                context = null;
                if (future != null) {
                    // 异常重连，重试连接
                    future.channel().eventLoop().schedule(this::start, RECONNECT_TRYING_INTERVAL, TimeUnit.SECONDS);
                }
            }
        }

        // MAC密钥交换认证，接收并验证 DH key
        private synchronized boolean authorize(ChannelHandlerContext ctx, byte[] bytes) {
            // 已经认证通过，此次非认证消息，返回失败
            if (authorized) {
                return false;
            }
            if (bytes.length <= 4) {
                return false;
            }
            try {
                int remoteId = BytesUtils.toInt(bytes);
                if (remoteId != REMOTE_ID) {
                    return false;
                }

                byte[] encodedDHPubKeyCertBytes = new byte[bytes.length - 4];
                System.arraycopy(bytes, 4, encodedDHPubKeyCertBytes, 0, bytes.length - 4);
                DHPubKeyCertificate certificate = MacKeyGenerator.resolveAndVerify(encodedDHPubKeyCertBytes, viewTopology.getStaticConf().getRSAPublicKey(remoteId));
                if (null == certificate) {
                    return false;
                }
                MacKey macKey = macKeyGen.exchange(certificate);
                attachChannelHandlerContext(ctx, macKey);
                LOGGER.debug("Outbound channel authorize success , me:{}, remoteID:{}", ME, REMOTE_ID);

                return true;
            } catch (Exception e) {
                LOGGER.debug("Outbound channel authorize error , me:{}, remoteID:{}", ME, REMOTE_ID, e);
            }

            return false;
        }
    }
}
