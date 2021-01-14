package bftsmart.communication.client;

import bftsmart.communication.client.netty.NettyClientServerCommunicationSystemServerSide;
import bftsmart.reconfiguration.ViewTopology;

/**
 * @author huanghaiquan
 *
 */
public class ClientCommunicationFactory {

    public static ClientCommunicationServerSide createServerSide(ViewTopology controller) {
        return new NettyClientServerCommunicationSystemServerSide(controller);
    }
}
