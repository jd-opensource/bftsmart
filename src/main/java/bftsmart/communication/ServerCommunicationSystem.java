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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import bftsmart.tom.core.messages.ViewMessage;
import bftsmart.tom.leaderchange.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.client.CommunicationSystemServerSide;
import bftsmart.communication.client.CommunicationSystemServerSideFactory;
import bftsmart.communication.client.RequestReceiver;
import bftsmart.communication.queue.MessageQueue;
import bftsmart.communication.queue.MessageQueueFactory;
import bftsmart.communication.server.ServersCommunicationLayer;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.core.messages.TOMMessage;

/**
 *
 * @author alysson
 */
public class ServerCommunicationSystem extends Thread {

	private boolean doWork = true;
	public static final long MESSAGE_WAIT_TIME = 100;
	private LinkedBlockingQueue<SystemMessage> inQueue = null;// new LinkedBlockingQueue<SystemMessage>(IN_QUEUE_SIZE);
	private MessageQueue messageInQueue;
	private MessageHandler messageHandler = new MessageHandler();
	private ServersCommunicationLayer serversConn;
	private volatile CommunicationSystemServerSide clientsConn;
	private ServerViewController controller;
	private final List<MessageHandlerRunner> messageHandlerRunners = new ArrayList<>();
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ServerCommunicationSystem.class);

	/**
	 * Creates a new instance of ServerCommunicationSystem
	 */
	public ServerCommunicationSystem(ServerViewController controller, ServiceReplica replica) throws Exception {
		super("Server CS");

		this.controller = controller;

		// 创建消息队列
		this.messageInQueue = MessageQueueFactory.newMessageQueue(MessageQueue.QUEUE_TYPE.IN,
				controller.getStaticConf().getInQueueSize());
		// 创建消息处理器
		// 遍历枚举类
		for (MessageQueue.MSG_TYPE msgType : MessageQueue.MSG_TYPE.values()) {
			this.messageHandlerRunners.add(new MessageHandlerRunner(msgType, messageInQueue, messageHandler));
		}

//        inQueue = new LinkedBlockingQueue<SystemMessage>(controller.getStaticConf().getInQueueSize());

		// create a new conf, with updated port number for servers
		// TOMConfiguration serversConf = new TOMConfiguration(conf.getProcessId(),
		// Configuration.getHomeDir(), "hosts.config");

		// serversConf.increasePortNumber();

		serversConn = new ServersCommunicationLayer(controller, messageInQueue, replica);

		// ******* EDUARDO BEGIN **************//
		// if (manager.isInCurrentView() || manager.isInInitView()) {
		clientsConn = CommunicationSystemServerSideFactory.getCommunicationSystemServerSide(controller);
		// }
		// ******* EDUARDO END **************//
		// start();
	}

	// ******* EDUARDO BEGIN **************//
	public void joinViewReceived() {
		serversConn.joinViewReceived();
	}

	public synchronized void updateServersConnections() {
		this.serversConn.updateConnections();
		if (clientsConn == null) {
			clientsConn = CommunicationSystemServerSideFactory.getCommunicationSystemServerSide(controller);
		}

	}

	// ******* EDUARDO END **************//
	public void setAcceptor(Acceptor acceptor) {
		messageHandler.setAcceptor(acceptor);
	}

	public void setTOMLayer(TOMLayer tomLayer) {
		messageHandler.setTOMLayer(tomLayer);
	}

	public synchronized void setRequestReceiver(RequestReceiver requestReceiver) {
		if (clientsConn == null) {
			clientsConn = CommunicationSystemServerSideFactory.getCommunicationSystemServerSide(controller);
		}
		clientsConn.setRequestReceiver(requestReceiver);
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
	@Override
	public void run() {

		if (doWork) {
			// 启动对应的消息队列处理器
			for (MessageHandlerRunner runner : messageHandlerRunners) {
				new Thread(runner, "MsgHandler-" + runner.msgType.name()).start();
			}
		}

//        long count = 0;
//        while (doWork) {
//            try {
//                if (count % 1000 == 0 && count > 0) {
//                    LOGGER.debug("(ServerCommunicationSystem.run) After " + count + " messages, inQueue size=" + inQueue.size());
//                }
//
//                SystemMessage sm = inQueue.poll(MESSAGE_WAIT_TIME, TimeUnit.MILLISECONDS);
//
//                if (sm != null) {
//                    LOGGER.debug("<-------receiving---------- " + sm);
//                    messageHandler.processData(sm);
//                    count++;
//                } else {
//                    messageHandler.verifyPending();
//                }
//            } catch (InterruptedException e) {
//                e.printStackTrace(System.err);
//            }
//        }
//        java.util.logging.Logger.getLogger(ServerCommunicationSystem.class.getName()).log(Level.INFO, "ServerCommunicationSystem stopped.");

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
			clientsConn.send(targets, (TOMMessage) sm, false);
		} else if (sm instanceof HeartBeatMessage) {
			// 心跳相关请求消息不做重发处理；
			LOGGER.debug("--------sending heart beat message with no retrying----------> {}", sm);
			serversConn.send(targets, sm, true, false);
		} else if (sm instanceof LeaderRequestMessage || sm instanceof LeaderResponseMessage) {
			// 从其他节点获取Leader信息的请求消息
			LOGGER.debug("--------sending leader res&resp message with no retrying----------> {}", sm);
			serversConn.send(targets, sm, true, false);
		} else if (sm instanceof LeaderStatusRequestMessage) {
			// 获取其他节点Leader状态的请求消息
			LOGGER.debug("--------sending leader status message with no retrying----------> {}", sm);
			serversConn.send(targets, sm, true, false);
		} else if (sm instanceof ViewMessage) {
			// 视图消息
			LOGGER.debug("--------sending view message with no retrying----------> {}", sm);
			serversConn.send(targets, sm, true, false);
		} else if (sm instanceof LCMessage) {
			// 领导者切换相关消息
			LOGGER.debug("--------sending leader change message with no retrying----------> {}", sm);
			serversConn.send(targets, sm, true, false);
		} else {
			LOGGER.debug("--------sending with retrying----------> {}", sm);
			serversConn.send(targets, sm, true);
		}
	}

	public void setServersConn(ServersCommunicationLayer serversConn) {
		this.serversConn = serversConn;
	}

	public ServersCommunicationLayer getServersConn() {
		return serversConn;
	}

	public CommunicationSystemServerSide getClientsConn() {
		return clientsConn;
	}

	@Override
	public String toString() {
		return serversConn.toString();
	}

	public void shutdown() {

		LOGGER.info("Shutting down communication layer");

		this.doWork = false;
		clientsConn.shutdown();
		serversConn.shutdown();

		// 关闭所有队列线程
		for (MessageHandlerRunner runner : messageHandlerRunners) {
			runner.shutdown();
		}
	}

	/**
	 * 消息处理线程
	 */
	private static class MessageHandlerRunner implements Runnable {

		public static final Logger LOGGER = LoggerFactory.getLogger(MessageHandlerRunner.class);

		/**
		 * 当前线程可处理的消息类型
		 */
		MessageQueue.MSG_TYPE msgType;

		/**
		 * 消息队列
		 */
		MessageQueue messageQueue;

		MessageHandler messageHandler;

		boolean doWork = true;

		public MessageHandlerRunner(MessageQueue.MSG_TYPE msgType, MessageQueue messageQueue,
				MessageHandler messageHandler) {
			this.msgType = msgType;
			this.messageQueue = messageQueue;
			this.messageHandler = messageHandler;
		}

		@Override
		public void run() {
			while (doWork) {
				SystemMessage sm = null;
				try {
					sm = messageQueue.poll(msgType, MESSAGE_WAIT_TIME, TimeUnit.MILLISECONDS);

					if (sm != null) {
						messageHandler.processData(sm);
					} else {
						messageHandler.verifyPending();
					}
				} catch (Throwable e) {
					if (sm == null) {
						String errMsg = String
								.format("Error occurred while process a null message! -- [HandlerType=%s]", msgType);
						LOGGER.error(errMsg, e);
					} else {
						String errMsg = String.format(
								"Error occurred while process message! -- %s [HandlerType=%s][MessageType=%s][MessageFrom=%s]",
								e.getMessage(), msgType, sm.getClass().getName(), sm.getSender());
						LOGGER.error(errMsg, e);
					}
				}
			}
		}

		public void shutdown() {
			this.doWork = false;
		}
	}
}
