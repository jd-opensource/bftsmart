package test.bftsmart.communication.server;

import bftsmart.communication.server.AbstractServersCommunicationLayer;
import bftsmart.communication.server.MessageConnection;
import bftsmart.reconfiguration.ViewTopology;

/**
	 * 基于队列对消息直接投递的通讯层实现；
	 * 
	 * @author huanghaiquan
	 *
	 */
	class MessageStreamCommunicationLayer extends AbstractServersCommunicationLayer {

		private MessageStreamNodeNetwork nodesNetwork;
		
		private MessageStreamNode currentNode;

		public MessageStreamCommunicationLayer(String realmName, ViewTopology topology, MessageStreamNodeNetwork nodesNetwork) {
			super(realmName, topology);
			this.currentNode = new MessageStreamNode(realmName, topology, messageInQueue);
			this.nodesNetwork = nodesNetwork;
			nodesNetwork.register(currentNode);
		}

		@Override
		protected void startCommunicationServer() {
		}

		@Override
		protected void closeCommunicationServer() {
		}

		@Override
		protected MessageConnection connectRemote(int remoteId) {
			return nodesNetwork.getConnectionToNode(remoteId);
		}

		@Override
		protected MessageConnection acceptRemote(int remoteId) {
			return nodesNetwork.getConnectionToNode(remoteId);
		}

	}