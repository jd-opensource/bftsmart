package test.bftsmart.communication.server;

import bftsmart.communication.server.AbstractServerCommunicationLayer;
import bftsmart.communication.server.MessageConnection;
import bftsmart.reconfiguration.ViewTopology;

/**
 * 基于队列对消息直接投递的通讯层实现；
 * 
 * @author huanghaiquan
 *
 */
class MessageStreamCommunicationLayer extends AbstractServerCommunicationLayer {

	private MessageStreamNodeNetwork nodesNetwork;

	private MessageStreamNode currentNode;

	public MessageStreamCommunicationLayer(String realmName, ViewTopology topology,
			MessageStreamNodeNetwork nodesNetwork) {
		super(realmName, topology);
		this.currentNode = new MessageStreamNode(realmName, topology.getCurrentProcessId());
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
		MessageStreamNode remoteNode = nodesNetwork.getNode(remoteId);
		MessageStreamNode currentNode = nodesNetwork.getNode(me);
		return new MessageStreamConnection(realmName, topology, remoteId, messageInQueue,
				remoteNode.requestInboundPipeline(me).getOutputStream(), currentNode.requestInboundPipeline(remoteId).getInputStream());
	}

	@Override
	protected MessageConnection acceptRemote(int remoteId) {
		MessageStreamNode remoteNode = nodesNetwork.getNode(remoteId);
		MessageStreamNode currentNode = nodesNetwork.getNode(me);
		return new MessageStreamConnection(realmName, topology, remoteId, messageInQueue,
				remoteNode.requestInboundPipeline(me).getOutputStream(), currentNode.requestInboundPipeline(remoteId).getInputStream());

	}

}