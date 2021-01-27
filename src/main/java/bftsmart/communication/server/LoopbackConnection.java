package bftsmart.communication.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.MacMessageCodec;
import bftsmart.communication.SystemMessage;
import bftsmart.communication.SystemMessageCodec;
import bftsmart.communication.queue.MessageQueue;

/**
 * 环回连接 ；
 * <p>
 * 
 * 连接到自身的连接；
 * 
 * @author huanghaiquan
 *
 */
public class LoopbackConnection implements MessageConnection {

	private static final Logger LOGGER = LoggerFactory.getLogger(LoopbackConnection.class);

	protected final String REALM_NAME;

	protected final int PROCESS_ID;

	private MessageQueue messageInQueue;

	private SystemMessageCodec messageCodec;

	public LoopbackConnection(String realmName, int processId, MessageQueue remoteQueue) {
		this.REALM_NAME = realmName;

		this.PROCESS_ID = processId;
		this.messageCodec = new SystemMessageCodec();
		this.messageCodec.setUseMac(false);

		this.messageInQueue = remoteQueue;
	}

	@Override
	public int getRemoteId() {
		return PROCESS_ID;
	}

	@Override
	public boolean isAlived() {
		return true;
	}

	@Override
	public MacMessageCodec<SystemMessage> getMessageCodec() {
		return messageCodec;
	}

	@Override
	public void start() {
		LOGGER.info("Start the loopback connection!  --[Id={}].",  PROCESS_ID);
	}

	@Override
	public void shutdown() {
		LOGGER.info("Shutdown the loopback connection! [Id={}]!", PROCESS_ID);
	}

	@Override
	public void clearOutQueue() {
	}

	@Override
	public AsyncFuture<SystemMessage, Void> send(SystemMessage message, boolean retrySending,
			CompletedCallback<SystemMessage, Void> callback) {
		message.authenticated = true;
		MessageQueue.SystemMessageType msgType = MessageQueue.SystemMessageType.typeOf(message);
		try {
			messageInQueue.put(msgType, message);
		} catch (InterruptedException e) {
			throw new IllegalStateException("Error occurred while sending message! --" + e.getMessage(), e);
		}

		AsyncFutureTask<SystemMessage, Void> future = new AsyncFutureTask<SystemMessage, Void>(message);
		future.setCallback(callback);
		future.complete(null);
		return future;
	}

	@Override
	public String toString() {
		return "Loopback Connection[Id=" + PROCESS_ID + "]";
	}

}
