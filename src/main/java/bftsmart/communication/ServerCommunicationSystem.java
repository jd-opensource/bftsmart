/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.communication;

import bftsmart.communication.client.CommunicationSystemServerSide;
import bftsmart.communication.client.RequestReceiver;
import bftsmart.communication.server.ServersCommunicationLayer;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.tom.core.TOMLayer;
import utils.concurrent.AsyncFuture;

/**
 *
 * @author alysson
 */
public interface ServerCommunicationSystem {

	// ******* EDUARDO BEGIN **************//
	void joinViewReceived();

	void updateServersConnections();

	// ******* EDUARDO END **************//
	void setAcceptor(Acceptor acceptor);
	
	Acceptor getAcceptor();

	void setTOMLayer(TOMLayer tomLayer);

	void setRequestReceiver(RequestReceiver requestReceiver);

	void setMessageHandler(MessageHandler messageHandler);

	MessageHandler getMessageHandler();

	/**
	 * Send a message to target processes. If the message is an instance of
	 * TOMMessage, it is sent to the clients, otherwise it is set to the servers.
	 *
	 * @param sm      the message to be sent
	 * @param targets the target receivers of the message
	 */
	default void send(SystemMessage sm, int... targets) {
		send(targets, sm);
	}

	/**
	 * Send a message to target processes. If the message is an instance of
	 * TOMMessage, it is sent to the clients, otherwise it is set to the servers.
	 *
	 * @param targets the target receivers of the message
	 * @param sm      the message to be sent
	 */
	void send(int[] targets, SystemMessage sm);

	void setServersConn(ServersCommunicationLayer serversConn);

	ServersCommunicationLayer getServersConn();

	CommunicationSystemServerSide getClientsConn();

	@Override
	String toString();

	void shutdown();

	AsyncFuture<Void> start();

//	void waitStopped();

}
