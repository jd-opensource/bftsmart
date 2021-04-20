package bftsmart.communication.impl.netty;

import bftsmart.communication.MessageQueue;
import bftsmart.reconfiguration.ViewTopology;

/**
 * Netty 接入连接
 */
public class NettyInboundConnection extends AbstractNettyConnection {

    public NettyInboundConnection(String realmName, ViewTopology viewTopology, int remoteId, MessageQueue messageInQueue) {
        super(realmName, viewTopology, remoteId, messageInQueue);
    }
}
