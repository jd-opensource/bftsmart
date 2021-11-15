package bftsmart.communication.impl.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;

public interface NettyChannelHandler {

    /**
     * 初始化通道
     *
     * @param channel
     */
    void initChannel(SocketChannel channel) throws Exception ;

    /**
     * 连接成功
     *
     * @param ctx
     */
    void channelActive(ChannelHandlerContext ctx);

    /**
     * 新消息
     *
     * @param ctx
     * @param msg
     */
    void channelRead(ChannelHandlerContext ctx, byte[] msg);

    /**
     * 连接断开
     *
     * @param ctx
     */
    void channelInactive(ChannelHandlerContext ctx);

    /**
     * 连接异常
     *
     * @param ctx
     * @param cause
     */
    void exceptionCaught(ChannelHandlerContext ctx, Throwable cause);
}
