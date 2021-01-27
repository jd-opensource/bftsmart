package test.bftsmart.communication.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.SystemMessage;
import bftsmart.communication.queue.MessageQueue;
import bftsmart.communication.queue.MessageQueueFactory;
import bftsmart.communication.queue.MessageQueue.SystemMessageType;
import bftsmart.communication.server.MessageListener;
import bftsmart.communication.server.ServerCommunicationLayer;
import bftsmart.communication.server.socket.SocketServerCommunicationLayer;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.ReplicaTopology;
import bftsmart.reconfiguration.ViewTopology;
import bftsmart.tom.ReplicaConfiguration;
import utils.security.RandomUtils;

public class ServerCommunicationLayerTest {

	private static Logger LOGGER = LoggerFactory.getLogger(ServerCommunicationLayerTest.class);

	@Test
	public void testQueueNodes() {
		final String realmName = "TEST-NET";
		int[] viewProcessIds = { 0, 1, 2, 3 };

		final MessageQueuesNetwork messageNetwork = new MessageQueuesNetwork();

		ServerCommunicationLayer[] servers = prepareQueueNodes(realmName, viewProcessIds, messageNetwork);
		MessageCounter[] counters = prepareMessageCounters(servers);

		for (ServerCommunicationLayer srv : servers) {
			srv.start();
		}

		// 生成待发送的测试消息；
		SystemMessage[] testMessages = prepareMessages(0, 6);

		// 从 server0 广播给全部的节点，包括 server0 自己；
		broadcast(servers[0], testMessages, viewProcessIds);

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
		// 验证所有节点都能完整地收到消息；
		for (MessageCounter counter : counters) {
			counter.assertMessagesEquals(testMessages);
			counter.clear();
		}
	}

	@Test
	public void testMessageStreamInputOutput() throws IOException, InterruptedException {
		final String realmName = "TEST-NET";
		int[] viewProcessIds = { 0, 1, 2, 3 };
		ViewTopology topology = CommunicationtTestMocker.mockTopology(viewProcessIds[0], viewProcessIds);
		MessageQueue messageInQueue = MessageQueueFactory.newMessageQueue(MessageQueue.QueueDirection.IN,
				topology.getStaticConf().getInQueueSize());
		MessageStreamNode node = new MessageStreamNode(realmName, topology, messageInQueue);

		DataOutputStream output = node.getOutputStream();
		DataInputStream input = node.getInputStream();

		// 测试在同步环境下，写入和读取的一致性；
		{
			ByteArrayOutputStream randomBytesBuff = new ByteArrayOutputStream();
			for (int i = 0; i < 2; i++) {
				byte[] bytes = RandomUtils.generateRandomBytes(4 * i + 6);
				output.write(bytes);
				randomBytesBuff.write(bytes);
			}
			output.flush();

			byte[] totalBytes = randomBytesBuff.toByteArray();

			ByteArrayOutputStream readBuffer = new ByteArrayOutputStream();
			int totalSize = totalBytes.length;
			byte[] bf = new byte[16];
			int len = 0;
			while ((len = input.read(bf)) > 0) {
				readBuffer.write(bf, 0, len);
				totalSize -= len;
				if (totalSize <= 0) {
					break;
				}
			}
			byte[] totalReadBytes = readBuffer.toByteArray();

			assertArrayEquals(totalBytes, totalReadBytes);
		}

		// 测试在异步环境下，写入和读取的一致性；
//		{
//			CyclicBarrier barrier = new CyclicBarrier(2);
//
//			ByteArrayOutputStream randomBytesBuff = new ByteArrayOutputStream();
//			Thread sendThrd = new Thread(new Runnable() {
//				@Override
//				public void run() {
//					try {
//						barrier.await();
//					} catch (InterruptedException | BrokenBarrierException e1) {
//						e1.printStackTrace();
//					}
//					try {
//
//						for (int i = 0; i < 10; i++) {
//							byte[] bytes = RandomUtils.generateRandomBytes(4 * i + 6);
//							output.write(bytes);
//							randomBytesBuff.write(bytes);
//						}
//						output.flush();
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//				}
//			});
//
//			boolean[] flag = { true };
//
//			ByteArrayOutputStream readBuffer = new ByteArrayOutputStream();
//			Thread recvThrd = new Thread(new Runnable() {
//				@Override
//				public void run() {
//					try {
//						barrier.await();
//					} catch (InterruptedException | BrokenBarrierException e1) {
//						e1.printStackTrace();
//					}
//					while (flag[0]) {
//						try {
//							byte[] bf = new byte[16];
//							int len = 0;
//							while ((len = input.read(bf)) > 0) {
//								readBuffer.write(bf, 0, len);
//							}
//						} catch (IOException e) {
//							e.printStackTrace();
//						}
//
//						try {
//							Thread.sleep(1000);
//						} catch (InterruptedException e) {
//						}
//					}
//				}
//			});
//
//			sendThrd.start();
//			recvThrd.start();
//
//			sendThrd.join();
//			Thread.sleep(1000);
//			flag[0] = false;
//			recvThrd.join();
//
//			byte[] totalBytes = randomBytesBuff.toByteArray();
//			byte[] totalReadBytes = readBuffer.toByteArray();
//
//			assertTrue(totalBytes.length > 0);
//			assertArrayEquals(totalBytes, totalReadBytes);
//		}
	}

	/**
	 * 测试当个节点的对消息的发送和接收；
	 */
	@Test
	public void testSingleStreamNode() {
		final String realmName = "TEST-NET";
		final int[] viewProcessIds = { 0, 1 };
		final MessageStreamNodeNetwork nodesNetwork = new MessageStreamNodeNetwork();

		ServerCommunicationLayer server0 = createStreamNode(realmName, 0, viewProcessIds, nodesNetwork);
		MessageCounter counter1 = prepareMessageCounter(server0);
		
		MessageStreamNode node0 = nodesNetwork.getNode(0);
		
		
		
	}

	/**
	 * 测试一组节点构成网络的消息广播发送和接收；
	 */
	@Test
	public void testStreamNodesNetwork() {
		final String realmName = "TEST-NET";
		final int[] viewProcessIds = { 0, 1 };

		final MessageStreamNodeNetwork nodesNetwork = new MessageStreamNodeNetwork();

		ServerCommunicationLayer[] servers = prepareStreamNodes(realmName, viewProcessIds, nodesNetwork);
		MessageCounter counter1 = prepareMessageCounter(servers[1]);

		for (ServerCommunicationLayer srv : servers) {
			srv.start();
		}

		// 生成待发送的测试消息；
		SystemMessage[] testMessages = prepareMessages(0, 1);

		// 从 server0 发送给 server1 ；
		broadcast(servers[0], testMessages, viewProcessIds[1]);

		// 验证节点都能完整地收到消息；
		try {
			counter1.assertMessagesEquals(testMessages);
			counter1.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testSocketNodesNetwork() {
		final String realmName = "TEST-NET";
		int[] viewProcessIds = { 0, 1, 2, 3 };
		int[] ports = { 15100, 15110, 15120, 15130 };

		ServerCommunicationLayer[] servers = prepareSocketNodes(realmName, viewProcessIds, ports);
		MessageCounter[] counters = prepareMessageCounters(servers);

		for (ServerCommunicationLayer srv : servers) {
			srv.start();
		}

		// 生成待发送的测试消息；
		SystemMessage[] testMessages = prepareMessages(0, 6);

		// 从 server0 广播给全部的节点，包括 server0 自己；
		broadcast(servers[0], testMessages, viewProcessIds);

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
		}
		// 验证所有节点都能完整地收到消息；
		for (MessageCounter counter : counters) {
			counter.assertMessagesEquals(testMessages);
			counter.clear();
		}
	}

	private ServerCommunicationLayer[] prepareQueueNodes(String realmName, int[] viewProcessIds,
			MessageQueuesNetwork messageNetwork) {
		ServerCommunicationLayer[] comLayers = new ServerCommunicationLayer[viewProcessIds.length];
		for (int i = 0; i < comLayers.length; i++) {
			ViewTopology topology = CommunicationtTestMocker.mockTopology(viewProcessIds[i], viewProcessIds);
			ServerCommunicationLayer comLayer = new MessageQueueCommunicationLayer(realmName, topology, messageNetwork);
			comLayers[i] = comLayer;
		}

		return comLayers;
	}

	private ServerCommunicationLayer[] prepareStreamNodes(String realmName, int[] viewProcessIds,
			MessageStreamNodeNetwork nodesNetwork) {
		ServerCommunicationLayer[] comLayers = new ServerCommunicationLayer[viewProcessIds.length];
		for (int i = 0; i < comLayers.length; i++) {
			ServerCommunicationLayer comLayer = createStreamNode(realmName, viewProcessIds[i], viewProcessIds,
					nodesNetwork);
			comLayers[i] = comLayer;
		}

		return comLayers;
	}

	private ServerCommunicationLayer createStreamNode(String realmName, int processId, int[] viewProcessIds,
			MessageStreamNodeNetwork nodesNetwork) {
		ReplicaConfiguration conf = CommunicationtTestMocker.mockDefaultConfiguration(processId, viewProcessIds);
		ViewTopology topology = CommunicationtTestMocker.mockTopology(processId, viewProcessIds, conf);
		ServerCommunicationLayer comLayer = new MessageStreamCommunicationLayer(realmName, topology, nodesNetwork);
		return comLayer;
	}

	private ServerCommunicationLayer[] prepareSocketNodes(String realmName, int[] viewProcessIds, int[] ports) {
		ServerCommunicationLayer[] comLayers = new ServerCommunicationLayer[viewProcessIds.length];
		for (int i = 0; i < comLayers.length; i++) {
			ReplicaTopology topology = CommunicationtTestMocker.mockTopologyWithTCP(viewProcessIds[i], viewProcessIds,
					ports);
			ServerCommunicationLayer comLayer = new SocketServerCommunicationLayer(realmName, topology);
			comLayers[i] = comLayer;
		}

		return comLayers;
	}

	private MessageCounter[] prepareMessageCounters(ServerCommunicationLayer[] comLayers) {
		MessageCounter[] counters = new MessageCounter[comLayers.length];
		for (int i = 0; i < counters.length; i++) {
			MessageCounter counter = new MessageCounter();
			comLayers[i].addMessageListener(SystemMessageType.CONSENSUS, counter);
			counters[i] = counter;
		}
		return counters;
	}

	private MessageCounter prepareMessageCounter(ServerCommunicationLayer comLayer) {
		MessageCounter counter = new MessageCounter();
		comLayer.addMessageListener(SystemMessageType.CONSENSUS, counter);
		return counter;
	}

	private void broadcast(ServerCommunicationLayer server, SystemMessage[] testMessages, int... targets) {
		for (int i = 0; i < testMessages.length; i++) {
			server.send(targets, testMessages[i], false);
		}
	}

	private SystemMessage[] prepareMessages(int fromProccessId, int count) {
		MessageFactory messageFactory = new MessageFactory(fromProccessId);
		SystemMessage[] messages = new SystemMessage[count];
		for (int i = 0; i < count; i++) {
			ConsensusMessage msg = messageFactory.createPropose(0, i, RandomUtils.generateRandomBytes(20));
			messages[i] = msg;
			assertEquals(fromProccessId, msg.getSender());
		}
		return messages;
	}

	private static class MessageCounter implements MessageListener {

		private List<SystemMessage> messages = Collections.synchronizedList(new LinkedList<>());

		@Override
		public void onReceived(SystemMessageType messageType, SystemMessage message) {
			messages.add(message);
		}

		public void clear() {
			messages.clear();
		}

		public void assertMessagesEquals(SystemMessage... expectedMessages) {
			SystemMessage[] actualMessages = messages.toArray(new SystemMessage[messages.size()]);
			assertEquals(expectedMessages.length, actualMessages.length);
			for (int i = 0; i < actualMessages.length; i++) {
				assertEquals(expectedMessages[i], actualMessages[i]);
			}
		}
	}

}
