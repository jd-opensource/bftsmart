package bftsmart.communication.client;

import bftsmart.communication.client.netty.NettyClientServerCommunicationSystemServerSide;
import bftsmart.reconfiguration.ServerViewController;

/**
 * @author huanghaiquan
 *
 */
public class ClientCommunicationFactory {

    public static CommunicationSystemServerSide createServerSide(ServerViewController controller) {
        return new NettyClientServerCommunicationSystemServerSide(controller);
    }
}
