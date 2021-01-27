package test.bftsmart.communication.server;

import bftsmart.communication.queue.MessageQueue;
import bftsmart.communication.server.AbstractServersCommunicationLayer;
import bftsmart.communication.server.MessageConnection;
import bftsmart.communication.server.LoopbackConnection;
import bftsmart.reconfiguration.ViewTopology;

/**
	 * 基于队列对消息直接投递的通讯层实现；
	 * 
	 * @author huanghaiquan
	 *
	 */
	class MessageQueueCommunicationLayer extends AbstractServersCommunicationLayer {

		private MessageQueuesNetwork messageNetwork;

		public MessageQueueCommunicationLayer(String realmName, ViewTopology topology, MessageQueuesNetwork messageNetwork) {
			super(realmName, topology);
			this.messageNetwork = messageNetwork;
			this.messageNetwork.register(me, messageInQueue);
		}

		@Override
		protected void startCommunicationServer() {
		}

		@Override
		protected void closeCommunicationServer() {
		}

		@Override
		protected MessageConnection connectRemote(int remoteId) {
			MessageQueue queue = messageNetwork.getQueueOfNode(remoteId);
			return new LoopbackConnection(realmName, remoteId, queue);
		}

		@Override
		protected MessageConnection acceptRemote(int remoteId) {
			MessageQueue queue = messageNetwork.getQueueOfNode(remoteId);
			return new LoopbackConnection(realmName, remoteId, queue);
		}

	}