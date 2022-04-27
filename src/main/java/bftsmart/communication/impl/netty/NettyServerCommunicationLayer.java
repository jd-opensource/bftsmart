package bftsmart.communication.impl.netty;

import bftsmart.communication.DHPubKeyCertificate;
import bftsmart.communication.MacKey;
import bftsmart.communication.MacKeyGenerator;
import bftsmart.communication.impl.AbstractCommunicationLayer;
import bftsmart.communication.impl.MessageConnection;
import bftsmart.reconfiguration.ViewTopology;
import bftsmart.util.SSLContextFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
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
import utils.net.SSLMode;
import utils.net.SSLSecurity;

import javax.net.ssl.SSLEngine;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 通讯层 Netty实现
 */
public class NettyServerCommunicationLayer extends AbstractCommunicationLayer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyServerCommunicationLayer.class);
    private final int MAX_MESSAGE_SIZE = 100 * 1024 * 1024;

    private NettyServer communicationServer;
    private Object addressLock = new Object();
    private Map<String, Integer> connectionAddresses = new HashMap<>();
    private Map<Integer, NettyInboundConnection> inboundConnections = new HashMap<>();
    private MacKeyGenerator macKeyGen;
    private SSLSecurity sslSecurity;

    public NettyServerCommunicationLayer(String realmName, ViewTopology topology) {
        this(realmName, topology, new SSLSecurity());
    }

    public NettyServerCommunicationLayer(String realmName, ViewTopology topology, SSLSecurity sslSecurity) {
        super(realmName, topology);
        this.sslSecurity = sslSecurity;
        this.macKeyGen = new MacKeyGenerator(topology.getStaticConf().getRSAPublicKey(),
                topology.getStaticConf().getRSAPrivateKey(), topology.getStaticConf().getDHG(),
                topology.getStaticConf().getDHP());
    }

    @Override
    protected void startCommunicationServer() {
        int port = topology.getStaticConf().getServerToServerPort(me);
        communicationServer = new NettyServer(port);
    }

    @Override
    protected void closeCommunicationServer() {
        NettyServer server = communicationServer;
        communicationServer = null;
        try {
            server.close();
        } catch (Exception e) {
            LOGGER.warn(String.format("Error occurred while closing netty server! --%s --[CurrentId=%s]",
                    e.getMessage(), me), e);
        }
    }

    @Override
    protected MessageConnection connectOutbound(int remoteId) {
        return new NettyOutboundConnection(realmName, topology, remoteId, messageInQueue, sslSecurity);
    }

    @Override
    protected MessageConnection acceptInbound(int remoteId) {
        NettyInboundConnection conn = inboundConnections.get(remoteId);
        if (conn == null) {
            if (conn == null) {
                conn = new NettyInboundConnection(realmName, topology, remoteId, messageInQueue);
                inboundConnections.put(remoteId, conn);
            }
        }
        return conn;
    }

    /**
     * 通讯层 Netty服务
     */
    public class NettyServer implements NettyChannelHandler, Closeable {

        private NioEventLoopGroup bossGroup, workerGroup;
        private ChannelFuture future;

        public NettyServer(int port) {
            bossGroup = new NioEventLoopGroup();
            workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);

            ServerBootstrap bootstrap = new ServerBootstrap();
            InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new NettyInitializerHandler(this))
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            future = bootstrap.bind(port);
            future.syncUninterruptibly();

            LOGGER.info("Netty server started with port: {}", port);
        }

        @Override
        public void close() throws IOException {
            try {
                if (future != null) {
                    future.channel().close().syncUninterruptibly();
                }
                future = null;
            } finally {
                if (workerGroup != null) {
                    workerGroup.shutdownGracefully().syncUninterruptibly();
                }
                if (bossGroup != null) {
                    bossGroup.shutdownGracefully().syncUninterruptibly();
                }
            }

            LOGGER.info("Netty server closed");
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception {
            if(topology.getStaticConf().isSecure(me)) {
                SSLEngine sslEngine = SSLContextFactory.getSSLContext(false, sslSecurity).createSSLEngine();
                sslEngine.setUseClientMode(false);
                if(null != sslSecurity.getEnabledProtocols() && sslSecurity.getEnabledProtocols().length > 0) {
                    sslEngine.setEnabledProtocols(sslSecurity.getEnabledProtocols());
                }
                if(null != sslSecurity.getCiphers() && sslSecurity.getCiphers().length > 0) {
                    sslEngine.setEnabledCipherSuites(sslSecurity.getCiphers());
                }

                sslEngine.setNeedClientAuth(sslSecurity.getSslMode(false).equals(SSLMode.TWO_WAY));

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
            LOGGER.debug("Inbound channel connected. me:{}, remote:{}", me, ctx.channel().remoteAddress());

            // 发送ID
            byte[] idBytes = BytesUtils.toBytes(me);
            // 发送MAC密钥交换信息，发送 DH key
            byte[] macBytes = macKeyGen.getDHPubKeyCertificate().getEncodedBytes();
            byte[] bytes = new byte[idBytes.length + macBytes.length];
            System.arraycopy(idBytes, 0, bytes, 0, idBytes.length);
            System.arraycopy(macBytes, 0, bytes, idBytes.length, macBytes.length);
            ctx.writeAndFlush(bytes);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, byte[] msg) {
            String address = ctx.channel().remoteAddress().toString();
            if (!connectionAddresses.containsKey(address)) {
                if (!authorize(ctx, address, msg)) {
                    if (connectionAddresses.containsKey(address)) {
                        inboundConnections.get(connectionAddresses.get(address)).receiveMessage(msg);
                    }
                }
            } else {
                inboundConnections.get(connectionAddresses.get(address)).receiveMessage(msg);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            synchronized (addressLock) {
                String remote = ctx.channel().remoteAddress().toString();
                LOGGER.debug("Outbound channel disconnected. me:{}, remote:{}", me, remote);
                connectionAddresses.remove(remote);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            synchronized (addressLock) {
                String remote = ctx.channel().remoteAddress().toString();
                LOGGER.error("Outbound channel exceptionCaught. me:{}, remote:{}", me, remote, cause);
                connectionAddresses.remove(remote);
            }
        }

        // MAC密钥交换认证，接收并验证 DH key
        private boolean authorize(ChannelHandlerContext ctx, String address, byte[] bytes) {
            synchronized (addressLock) {
                // 已经认证通过，此次非认证消息，返回失败
                if (connectionAddresses.containsKey(address)) {
                    return false;
                }
                if (bytes.length <= 4) {
                    return false;
                }

                int remoteId = -1;
                try {
                    remoteId = BytesUtils.toInt(bytes);
                    if (!inboundConnections.containsKey(remoteId)) {
                        return false;
                    }

                    byte[] encodedDHPubKeyCertBytes = new byte[bytes.length - 4];
                    System.arraycopy(bytes, 4, encodedDHPubKeyCertBytes, 0, bytes.length - 4);
                    DHPubKeyCertificate certificate = MacKeyGenerator.resolveAndVerify(encodedDHPubKeyCertBytes, topology.getStaticConf().getRSAPublicKey(remoteId));
                    if (null == certificate) {
                        return false;
                    }
                    MacKey macKey = macKeyGen.exchange(certificate);
                    inboundConnections.get(remoteId).attachChannelHandlerContext(ctx, macKey);
                    connectionAddresses.put(address, remoteId);
                    LOGGER.debug("Inbound channel authorize success , me:{}, remoteID:{}", me, remoteId);

                    return true;
                } catch (Exception e) {
                    LOGGER.error("Inbound channel authorize error , me:{}, remoteID:{}", me, remoteId, e);
                }

                return false;
            }
        }

    }
}
