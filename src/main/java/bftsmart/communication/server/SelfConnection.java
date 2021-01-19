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
package bftsmart.communication.server;

import javax.crypto.SecretKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.SystemMessage;
import bftsmart.communication.queue.MessageQueue;
import bftsmart.reconfiguration.ViewTopology;

/**
 * This class represents a connection with other server.
 *
 * ServerConnections are created by ServerCommunicationLayer.
 *
 * @author alysson
 */
public class SelfConnection implements MessageConnection {

	private static final Logger LOGGER = LoggerFactory.getLogger(SelfConnection.class);

	// 重连周期
	private static final long POOL_INTERVAL = 5000;

	private final String REALM_NAME;

	private ViewTopology viewTopology;
	private int remoteId;
	private MessageQueue messageInQueue;

	public SelfConnection(String realmName, ViewTopology viewTopology, MessageQueue messageInQueue) {
		this.REALM_NAME = realmName;
		this.viewTopology = viewTopology;

		this.remoteId = viewTopology.getCurrentProcessId();

		this.messageInQueue = messageInQueue;
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
	public SecretKey getSecretKey() {
		return null;
	}

	/**
	 * Stop message sending and reception.
	 */
	@Override
	public void shutdown() {
		LOGGER.info("SHUTDOWN for {}", remoteId);
	}

	@Override
	public AsyncFuture<SystemMessage, Void> send(SystemMessage message, boolean useMAC, boolean retrySending,
			CompletedCallback<SystemMessage, Void> callback) {
		message.authenticated = true;
		MessageQueue.SystemMessageType msgType = MessageQueue.SystemMessageType.typeOf(message);
		try {
			messageInQueue.put(msgType, message);
		} catch (InterruptedException e) {
			throw new IllegalStateException("Error occurred while sending message! --" + e.getMessage(), e);
		}

		AsyncFutureTask<SystemMessage, Void> future = new AsyncFutureTask<SystemMessage, Void>(message);
		future.complete(null);
		return future;
	}

	@Override
	public String toString() {
		return "SelfConnection[RemoteID: " + remoteId + "]";
	}

}
