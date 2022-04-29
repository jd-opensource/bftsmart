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

import bftsmart.communication.impl.netty.NettyServerCommunicationLayer;
import org.slf4j.LoggerFactory;

import bftsmart.communication.client.ClientCommunicationServerSide;
import bftsmart.communication.impl.MessageListener;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.ViewTopology;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.ViewMessage;
import bftsmart.tom.leaderchange.HeartBeatMessage;
import bftsmart.tom.leaderchange.LCMessage;
import bftsmart.tom.leaderchange.LeaderRequestMessage;
import bftsmart.tom.leaderchange.LeaderResponseMessage;
import bftsmart.tom.leaderchange.LeaderStatusRequestMessage;
import utils.concurrent.AsyncFuture;
import utils.concurrent.CompletableAsyncFuture;
import utils.net.SSLSecurity;

/**
 *
 * @author alysson
 */
public class ServerCommunicationSystemImpl implements ServerCommunicationSystem {

	private boolean doWork = true;
//	public static final long MESSAGE_WAIT_TIME = 100;
//	private MessageQueue messageInQueue;
	private MessageHandler messageHandler;// = new MessageHandler();
	private CommunicationLayer serversCommunication;
	private final ClientCommunicationServerSide clientCommunication;
	private ViewTopology controller;
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ServerCommunicationSystemImpl.class);

	/**
	 * Creates a new instance of ServerCommunicationSystem
	 */
	public ServerCommunicationSystemImpl(ClientCommunicationServerSide clientCommunication,
										 MessageHandler messageHandler, ServerViewController controller, String realmName) {
		this(clientCommunication, messageHandler, controller, realmName, new SSLSecurity());
	}

	public ServerCommunicationSystemImpl(ClientCommunicationServerSide clientCommunication,
										 MessageHandler messageHandler, ServerViewController controller, String realmName, SSLSecurity sslSecurity) {
		this.clientCommunication = clientCommunication;
		this.messageHandler = messageHandler;
		this.controller = controller;

		this.serversCommunication = new NettyServerCommunicationLayer(realmName, controller, sslSecurity);

		// 创建消息处理器
		// 遍历枚举类
		for (MessageQueue.SystemMessageType msgType : MessageQueue.SystemMessageType.values()) {
			MessageHandlerAdapter handler;
			switch (msgType) {
			case CONSENSUS:
				handler = new ConsensusMessageHandler(messageHandler);
				break;
			case HEART:
				handler = new HeartbeatMessageHandler(messageHandler);
				break;
			case LC:
				handler = new LCMessageHandler(messageHandler);
				break;

			default:
				throw new IllegalStateException("Unsupport Message Type[" + msgType + "]!");
			}
			this.serversCommunication.addMessageListener(msgType, handler);
		}
	}

	public synchronized void updateServersConnections() {
		this.serversCommunication.updateConnections();
	}

	public void setMessageHandler(MessageHandler messageHandler) {
		this.messageHandler = messageHandler;
	}

	public MessageHandler getMessageHandler() {
		return messageHandler;
	}

	/**
	 * Thread method responsible for receiving messages sent by other servers.
	 */
	private void startMessageHandle() {

		if (!doWork) {
			return;
		}

		messageHandler.getAcceptor().start();
	}

	/**
	 * Send a message to target processes. If the message is an instance of
	 * TOMMessage, it is sent to the clients, otherwise it is set to the servers.
	 *
	 * @param sm      the message to be sent
	 * @param targets the target receivers of the message
	 */
	public void send(SystemMessage sm, int... targets) {
		if (targets == null || targets.length == 0) {
			LOGGER.warn("No target to send system message[{}] from node[{}]!", sm.getClass().getName(), sm.getSender());
			return;
		}
		send(targets, sm);
	}

	/**
	 * Send a message to target processes. If the message is an instance of
	 * TOMMessage, it is sent to the clients, otherwise it is set to the servers.
	 *
	 * @param targets the target receivers of the message
	 * @param sm      the message to be sent
	 */
	public void send(int[] targets, SystemMessage sm) {
		if (sm instanceof TOMMessage) {
			clientCommunication.send(targets, (TOMMessage) sm, false);
		} else if (sm instanceof HeartBeatMessage) {
			// 心跳相关请求消息不做重发处理；
			LOGGER.debug("--------sending heart beat message with no retrying----------> {}", sm);
			serversCommunication.send(targets, sm, false);
		} else if (sm instanceof LeaderRequestMessage || sm instanceof LeaderResponseMessage) {
			// 从其他节点获取Leader信息的请求消息
			LOGGER.debug("--------sending leader res&resp message with no retrying----------> {}", sm);
			serversCommunication.send(targets, sm, false);
		} else if (sm instanceof LeaderStatusRequestMessage) {
			// 获取其他节点Leader状态的请求消息
			LOGGER.debug("--------sending leader status message with no retrying----------> {}", sm);
			serversCommunication.send(targets, sm,  false);
		} else if (sm instanceof ViewMessage) {
			// 视图消息
			LOGGER.debug("--------sending view message with no retrying----------> {}", sm);
			serversCommunication.send(targets, sm,  false);
		} else if (sm instanceof LCMessage) {
			// 领导者切换相关消息
			LOGGER.debug("--------sending leader change message with no retrying----------> {}", sm);
			serversCommunication.send(targets, sm, false);
		} else {
			LOGGER.debug("--------sending with retrying----------> {}", sm);
			serversCommunication.send(targets, sm, true);
		}
	}

	@Override
	public CommunicationLayer getServersCommunication() {
		return serversCommunication;
	}

	@Override
	public ClientCommunicationServerSide getClientCommunication() {
		return clientCommunication;
	}

	@Override
	public String toString() {
		return serversCommunication.toString();
	}

	public void shutdown() {
		if (!doWork) {
			return;
		}
		LOGGER.info("Shutting down server communication layer");

		doWork = false;

		try {
			clientCommunication.shutdown();
		} catch (Exception e) {
			LOGGER.warn("Client Connections shutdown error of node[" + controller.getCurrentProcessId() + "]! --"
					+ e.getMessage(), e);
		}
		try {
			serversCommunication.close();
		} catch (Exception e) {
			LOGGER.warn("Server Connections shutdown error of node[" + controller.getCurrentProcessId() + "]! --"
					+ e.getMessage(), e);
		}
		try {
			messageHandler.getAcceptor().shutdown();
		} catch (Exception e) {
			LOGGER.warn(
					"Acceptor shutdown error of node[" + controller.getCurrentProcessId() + "]! --" + e.getMessage(),
					e);
		}
	}

	/**
	 * 消息处理线程
	 */
	private abstract class MessageHandlerAdapter implements MessageListener {

		/**
		 * 当前线程可处理的消息类型
		 */
		private final MessageQueue.SystemMessageType MSG_TYPE;

		public MessageHandlerAdapter(MessageQueue.SystemMessageType msgType) {
			this.MSG_TYPE = msgType;
		}

		protected abstract void processMessage(SystemMessage sm);

		@Override
		public void onReceived(SystemMessage message) {
			try {
				processMessage(message);
			} catch (Throwable e) {
				String errMsg = String.format(
						"Error occurred while handling message! -- %s [HandlerType=%s][MessageType=%s][MessageFrom=%s]",
						e.getMessage(), MSG_TYPE, message.getClass().getName(), message.getSender());
				LOGGER.error(errMsg, e);
			}
		}

	}

	/**
	 * 消息处理线程
	 */
	private class ConsensusMessageHandler extends MessageHandlerAdapter {

		private MessageHandler messageHandler;

		public ConsensusMessageHandler(MessageHandler messageHandler) {
			super(MessageQueue.SystemMessageType.CONSENSUS);
			this.messageHandler = messageHandler;
		}

		@Override
		protected void processMessage(SystemMessage sm) {
			if (sm != null) {
				messageHandler.processData(sm);
			} else {
				// TODO: 优化潜在缺陷：当不传入 null 值的时候将不会触发对过期消息的处理；
				messageHandler.verifyPending();
			}
		}
	}

	/**
	 * 消息处理线程
	 */
	private class HeartbeatMessageHandler extends MessageHandlerAdapter {

		private MessageHandler messageHandler;

		public HeartbeatMessageHandler(MessageHandler messageHandler) {
			super(MessageQueue.SystemMessageType.HEART);
			this.messageHandler = messageHandler;
		}

		@Override
		protected void processMessage(SystemMessage sm) {
			if (sm != null) {
				messageHandler.processData(sm);
			}
		}
	}

	/**
	 * 消息处理线程
	 */
	private class LCMessageHandler extends MessageHandlerAdapter {

		private MessageHandler messageHandler;

		public LCMessageHandler(MessageHandler messageHandler) {
			super(MessageQueue.SystemMessageType.LC);
			this.messageHandler = messageHandler;
		}

		@Override
		protected void processMessage(SystemMessage sm) {
			if (sm != null) {
				messageHandler.processData(sm);
			}
		}
	}

	@Override
	public AsyncFuture<Void> start() {
		startMessageHandle();
		serversCommunication.start();
		return CompletableAsyncFuture.completeFuture(null);
	}
}
