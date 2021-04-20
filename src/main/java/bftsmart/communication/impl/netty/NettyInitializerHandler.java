package bftsmart.communication.impl.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class NettyInitializerHandler extends ChannelInitializer<SocketChannel> {

    private final NettyChannelHandler handler;

    public NettyInitializerHandler(NettyChannelHandler handler) {
        this.handler = handler;
    }

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
        handler.initChannel(channel);
    }
}
