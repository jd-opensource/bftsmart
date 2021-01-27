package bftsmart.communication.server;

import javax.crypto.SecretKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.MacMessageCodec;
import bftsmart.communication.MessageCodec;
import bftsmart.communication.SystemMessage;
import bftsmart.communication.queue.MessageQueue;

public class MessageQueueConnection implements MessageConnection {

	private static final Logger LOGGER = LoggerFactory.getLogger(MessageQueueConnection.class);

	protected final String REALM_NAME;

	protected final int remoteId;

	private MessageQueue messageInQueue;

	public MessageQueueConnection(String realmName, int remoteId, MessageQueue remoteQueue) {
		this.REALM_NAME = realmName;

		this.remoteId = remoteId;

		this.messageInQueue = remoteQueue;
	}

	@Override
	public int getRemoteId() {
		return remoteId;
	}

	@Override
	public boolean isAlived() {
		return true;
	}

	@Override
	public MacMessageCodec<SystemMessage> getMessageCodec() {
		return null;
	}
	
	@Override
	public void start() {
		LOGGER.info("Start the connection to remote[{}].", remoteId);
	}

	@Override
	public void shutdown() {
		LOGGER.info("Shutdown the connection to remote[{}]!", remoteId);
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
		return "SelfConnection[RemoteID: " + remoteId + "]";
	}

}
