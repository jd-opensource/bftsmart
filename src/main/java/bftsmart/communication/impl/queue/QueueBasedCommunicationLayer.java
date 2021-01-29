package bftsmart.communication.impl.queue;

import bftsmart.communication.MessageQueue;
import bftsmart.communication.impl.AbstractCommunicationLayer;
import bftsmart.communication.impl.LoopbackConnection;
import bftsmart.communication.impl.MessageConnection;
import bftsmart.reconfiguration.ViewTopology;

/**
 * 基于队列对消息直接投递的通讯层实现；
 * 
 * @author huanghaiquan
 *
 */
public class QueueBasedCommunicationLayer extends AbstractCommunicationLayer {

	private MessageQueueManager messageNetwork;

	public QueueBasedCommunicationLayer(String realmName, ViewTopology topology,
			MessageQueueManager messageNetwork) {
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
	protected MessageConnection connectOutbound(int remoteId) {
		MessageQueue queue = messageNetwork.getQueue(remoteId);
		return new LoopbackConnection(realmName, remoteId, queue);
	}

	@Override
	protected MessageConnection acceptInbound(int remoteId) {
		MessageQueue queue = messageNetwork.getQueue(remoteId);
		return new LoopbackConnection(realmName, remoteId, queue);
	}

}