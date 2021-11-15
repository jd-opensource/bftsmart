package bftsmart.communication.client;

import bftsmart.communication.client.netty.NettyClientServerCommunicationSystemServerSide;
import bftsmart.reconfiguration.ViewTopology;
import utils.net.SSLSecurity;

/**
 * @author huanghaiquan
 *
 */
public class ClientCommunicationFactory {

    public static ClientCommunicationServerSide createServerSide(ViewTopology controller) {
        return createServerSide(controller, new SSLSecurity());
    }

    public static ClientCommunicationServerSide createServerSide(ViewTopology controller, SSLSecurity sslSecurity) {
        return new NettyClientServerCommunicationSystemServerSide(controller, sslSecurity);
    }
}
