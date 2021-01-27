package test.bftsmart.communication.server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import bftsmart.communication.queue.MessageQueue;
import bftsmart.communication.server.AbstractStreamConnection;
import bftsmart.reconfiguration.ViewTopology;

public class MessageStreamConnection extends AbstractStreamConnection{
	
	private DataOutputStream sendOut;
	
	private DataInputStream recevIn;

	/**
	 * @param realmName 共识域
	 * @param viewTopology 视图拓扑
	 * @param remoteId 远端节点 Id；
	 * @param messageInQueue 当前连接的消息接收队列；
	 * @param sendOut 向远端节点发送消息的输出流；
	 * @param recevIn 从远端节点
	 */
	public MessageStreamConnection(String realmName, ViewTopology viewTopology, int remoteId,
			MessageQueue messageInQueue, DataOutputStream sendOut, DataInputStream recevIn) {
		super(realmName, viewTopology, remoteId, messageInQueue);
		this.sendOut = sendOut;
		this.recevIn = recevIn;
	}

	@Override
	public boolean isAlived() {
		return false;
	}

	@Override
	protected void rebuildConnection(long timeoutMillis) throws IOException {
	}

	@Override
	protected void closeConnection() {
	}

	@Override
	protected DataOutputStream getOutputStream() {
		return sendOut;
	}

	@Override
	protected DataInputStream getInputStream() {
		return recevIn;
	}

}
