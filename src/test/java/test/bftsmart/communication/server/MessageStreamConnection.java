package test.bftsmart.communication.server;

import java.io.InputStream;
import java.io.OutputStream;

import bftsmart.communication.queue.MessageQueue;
import bftsmart.communication.server.AbstractStreamConnection;
import bftsmart.communication.server.IOChannel;
import bftsmart.reconfiguration.ViewTopology;

public class MessageStreamConnection extends AbstractStreamConnection{
	
	private IOChannel ioChannel;

	/**
	 * @param realmName 共识域
	 * @param viewTopology 视图拓扑
	 * @param remoteId 远端节点 Id；
	 * @param messageInQueue 当前连接的消息接收队列；
	 * @param sendOut 向远端节点发送消息的输出流；
	 * @param recevIn 从远端节点
	 */
	public MessageStreamConnection(String realmName, ViewTopology viewTopology, int remoteId,
			MessageQueue messageInQueue, OutputStream sendOut, InputStream recevIn) {
		super(realmName, viewTopology, remoteId, messageInQueue);
		
		this.ioChannel = new IOChannel(recevIn, sendOut);
	}

	@Override
	public boolean isAlived() {
		return !ioChannel.isClosed();
	}

	@Override
	protected IOChannel getIOChannel(long timeoutMillis) {
		return ioChannel;
	}

}
