package test.bftsmart.communication.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import bftsmart.communication.impl.netty.NettyServerCommunicationLayer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.CommunicationLayer;
import bftsmart.communication.MacMessageCodec;
import bftsmart.communication.SystemMessage;
import bftsmart.communication.MessageQueue.SystemMessageType;
import bftsmart.communication.impl.MessageListener;
import bftsmart.communication.impl.queue.MessageQueueManager;
import bftsmart.communication.impl.queue.QueueBasedCommunicationLayer;
import bftsmart.communication.impl.socket.SocketServerCommunicationLayer;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.ReplicaTopology;
import bftsmart.reconfiguration.ViewTopology;
import bftsmart.tom.ReplicaConfiguration;
import utils.io.BytesUtils;
import utils.security.RandomUtils;
import utils.serialize.binary.BinarySerializeUtils;

public class ServerCommunicationLayerTest {

	private static Logger LOGGER = LoggerFactory.getLogger(ServerCommunicationLayerTest.class);

	@Test
	public void testQueueNodes() {
		final String realmName = "TEST-NET";
		int[] viewProcessIds = { 0, 1, 2, 3 };

		final MessageQueueManager messageNetwork = new MessageQueueManager();

		CommunicationLayer[] servers = prepareQueueNodes(realmName, viewProcessIds, messageNetwork);
		MessageCounter[] counters = prepareMessageCounters(servers);

		for (CommunicationLayer srv : servers) {
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

		closeServers(servers);
	}

	/**
	 * 测试当个节点的对消息的发送和接收；
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testDoubleStreamNodes() throws IOException, InterruptedException {
		final String realmName = "TEST-NET";
		final int[] viewProcessIds = { 0, 1 };
		final MessageStreamNodeNetwork nodesNetwork = new MessageStreamNodeNetwork();

		CommunicationLayer server0 = createStreamNode(realmName, 0, viewProcessIds, nodesNetwork);
		MessageCounter counter0 = prepareMessageCounter(server0);
		CommunicationLayer server1 = createStreamNode(realmName, 1, viewProcessIds, nodesNetwork);
		MessageCounter counter1 = prepareMessageCounter(server1);

		server0.start();
		server1.start();

		MessageStreamNode node0 = nodesNetwork.getNode(0);

		// 生成待测试发送的消息；
		ConsensusMessage msg_from_0 = createTestMessage(0);
		ConsensusMessage msg_from_1 = createTestMessage(1);

		// 测试自我发送的正确性；
		server0.send(msg_from_0, 0);
		server1.send(msg_from_1, 1);

		Thread.sleep(1000);// 稍等待一下，避免接收线程未来得及处理；
		counter0.assertMessagesEquals(msg_from_0);
		counter1.assertMessagesEquals(msg_from_1);
		counter0.clear();
		counter1.clear();

		// 测试对端消息接收的处理的正确性；
		// 模拟发送给节点 1；
		MacMessageCodec<SystemMessage> msgCodec1 = server0.getMessageCodec(1);// 取得用于发送给远端节点 1 的编解码器；
		byte[] messageBytes0 = msgCodec1.encode(msg_from_0);// 编码消息；

		// 发送编码消息；
		MessageStreamNode node1 = nodesNetwork.getNode(1);
		BytesUtils.writeInt(messageBytes0.length, node1.requestInboundPipeline(0).getOutputStream());
		node1.requestInboundPipeline(0).write(messageBytes0);

		// 检测 ServerCommunicationLayer 是否接收到消息；
		Thread.sleep(100);
		counter1.assertMessagesEquals(msg_from_0);

		closeServers(server0, server1);
	}

	/**
	 * 测试一组节点构成网络的消息广播发送和接收；
	 *
	 * @throws InterruptedException
	 */
	@Test
	public void testStreamNodesNetwork() throws InterruptedException {
		final String realmName = "TEST-NET";
		final int[] viewProcessIds = { 0, 1, 2, 3 };
		final MessageStreamNodeNetwork nodesNetwork = new MessageStreamNodeNetwork();

		CommunicationLayer[] servers = prepareStreamNodes(realmName, viewProcessIds, nodesNetwork);
		MessageCounter[] counters = prepareMessageCounters(servers);

		startServers(servers);

		// 生成待测试发送的消息；
		ConsensusMessage msg_from_0 = createTestMessage(0);

		// 测试直接从底层的通讯队列中写入数据，验证接收处理是否正确；
		directSend(msg_from_0, servers[0], nodesNetwork, 1, 2, 3);

		Thread.sleep(200);// 稍等待一下，避免接收线程未来得及处理；
		counters[1].assertMessagesEquals(msg_from_0);
		counters[1].clear();
		counters[2].assertMessagesEquals(msg_from_0);
		counters[2].clear();
		counters[3].assertMessagesEquals(msg_from_0);
		counters[3].clear();

		// 测试完整的广播发送和接收处理，验证整体的正确性；
		servers[0].send(msg_from_0, viewProcessIds);
		Thread.sleep(200);
		assertMessageCounters(counters, msg_from_0);
	}

	private void directSend(SystemMessage message, CommunicationLayer sender,
			MessageStreamNodeNetwork nodesNetwork, int... targets) {
		for (int i = 0; i < targets.length; i++) {
			MacMessageCodec<SystemMessage> msgCodec = sender.getMessageCodec(targets[i]);// 取得用于发送给远端节点 1 的编解码器；
			byte[] messageBytes = msgCodec.encode(message);// 编码消息；
			MessageStreamNode node = nodesNetwork.getNode(targets[i]);
			BytesUtils.writeInt(messageBytes.length, node.requestInboundPipeline(sender.getId()).getOutputStream());
			node.requestInboundPipeline(sender.getId()).write(messageBytes);
		}
	}


	/**
	 * 测试当个节点的对消息的发送和接收；
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testDoubleTCPNodes() throws IOException, InterruptedException {
		final String realmName = "TEST-NET";
		int[] viewProcessIds = { 0, 1};
		int[] ports = { 14100, 14110};

		CommunicationLayer[] servers = prepareSocketNodes(realmName, viewProcessIds, ports);
		MessageCounter[] counters = prepareMessageCounters(servers);

		for (CommunicationLayer server : servers) {
			server.start();
		}

		// 生成待测试发送的消息；
		ConsensusMessage msg_from_0 = createTestMessage(0);
		ConsensusMessage msg_from_1 = createTestMessage(1);

		// 测试自我发送的正确性；
		servers[0].send(msg_from_0, 0);
		servers[1].send(msg_from_1, 1);

		Thread.sleep(100);// 稍等待一下，避免接收线程未来得及处理；
		counters[0].assertMessagesEquals(msg_from_0);
		counters[1].assertMessagesEquals(msg_from_1);
		counters[0].clear();
		counters[1].clear();

		closeServers(servers);
	}

	@Test
	public void testSocketNodesNetwork() {
		final String realmName = "TEST-NET";
		int[] viewProcessIds = { 0, 1, 2, 3 };
		int[] ports = { 15100, 15110, 15120, 15130 };

		CommunicationLayer[] servers = prepareSocketNodes(realmName, viewProcessIds, ports);
		MessageCounter[] counters = prepareMessageCounters(servers);

		for (CommunicationLayer srv : servers) {
			srv.start();
		}

		// 生成待发送的测试消息；
		SystemMessage[] testMessages = prepareMessages(0, 1);

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

		closeServers(servers);
	}

	@Test
	public void testDoubleNettyNodes() throws InterruptedException {
		final String realmName = "TEST-NET";
		int[] viewProcessIds = { 0, 1};
		int[] ports = { 14100, 14110};

		CommunicationLayer[] servers = prepareNettyNodes2(realmName, viewProcessIds, ports);
		MessageCounter[] counters = prepareMessageCounters(servers);

		for (CommunicationLayer server : servers) {
			server.start();
		}

		// 生成待测试发送的消息；
		ConsensusMessage msg_from_0 = createTestMessage(0);
		ConsensusMessage msg_from_1 = createTestMessage(1);

		// 测试自我发送的正确性；
		servers[0].send(msg_from_0, 0);
		servers[1].send(msg_from_1, 1);

		Thread.sleep(1000);// 稍等待一下，避免接收线程未来得及处理；
		counters[0].assertMessagesEquals(msg_from_0);
		counters[1].assertMessagesEquals(msg_from_1);
		counters[0].clear();
		counters[1].clear();

		closeServers(servers);
	}

	@Test
	public void testNettyNodesNetwork() {
		final String realmName = "TEST-NET";
		int[] viewProcessIds = { 0, 1, 2, 3 };
		int[] ports = { 15100, 15110, 15120, 15130 };

		CommunicationLayer[] servers = prepareNettyNodes2(realmName, viewProcessIds, ports);
		MessageCounter[] counters = prepareMessageCounters(servers);

		for (CommunicationLayer srv : servers) {
			srv.start();
		}

		// 生成待发送的测试消息；
		SystemMessage[] testMessages = prepareMessages(0, 1);

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

		closeServers(servers);
	}

	private void startServers(CommunicationLayer... servers) {
		for (CommunicationLayer server : servers) {
			server.start();
		}
	}

	private void closeServers(CommunicationLayer... servers) {
		for (CommunicationLayer server : servers) {
			server.close();
		}
	}

	private void assertMessageCounters(MessageCounter[] counters, SystemMessage... testMessages) {
		for (MessageCounter counter : counters) {
			counter.assertMessagesEquals(testMessages);
			counter.clear();
		}
	}

	private ConsensusMessage createTestMessage(int senderId) {
		MessageFactory messageFactory = new MessageFactory(senderId);
		return messageFactory.createPropose(0, 0, RandomUtils.generateRandomBytes(100));
	}

	private CommunicationLayer[] prepareQueueNodes(String realmName, int[] viewProcessIds,
			MessageQueueManager messageNetwork) {
		CommunicationLayer[] comLayers = new CommunicationLayer[viewProcessIds.length];
		for (int i = 0; i < comLayers.length; i++) {
			ViewTopology topology = CommunicationtTestMocker.mockTopology(viewProcessIds[i], viewProcessIds);
			CommunicationLayer comLayer = new QueueBasedCommunicationLayer(realmName, topology, messageNetwork);
			comLayers[i] = comLayer;
		}

		return comLayers;
	}

	private CommunicationLayer[] prepareStreamNodes(String realmName, int[] viewProcessIds,
			MessageStreamNodeNetwork nodesNetwork) {
		CommunicationLayer[] comLayers = new CommunicationLayer[viewProcessIds.length];
		for (int i = 0; i < comLayers.length; i++) {
			CommunicationLayer comLayer = createStreamNode(realmName, viewProcessIds[i], viewProcessIds,
					nodesNetwork);
			comLayers[i] = comLayer;
		}

		return comLayers;
	}

	private CommunicationLayer createStreamNode(String realmName, int processId, int[] viewProcessIds,
			MessageStreamNodeNetwork nodesNetwork) {
		ReplicaConfiguration conf = CommunicationtTestMocker.mockDefaultConfiguration(processId, viewProcessIds);
		ViewTopology topology = CommunicationtTestMocker.mockTopology(processId, viewProcessIds, conf);
		CommunicationLayer comLayer = new MessageStreamCommunicationLayer(realmName, topology, nodesNetwork);
		return comLayer;
	}

	private CommunicationLayer[] prepareSocketNodes(String realmName, int[] viewProcessIds, int[] ports) {
		CommunicationLayer[] comLayers = new CommunicationLayer[viewProcessIds.length];
		for (int i = 0; i < comLayers.length; i++) {
			ReplicaTopology topology = CommunicationtTestMocker.mockTopologyWithTCP(viewProcessIds[i], viewProcessIds,
					ports);
			CommunicationLayer comLayer = new SocketServerCommunicationLayer(realmName, topology);
			comLayers[i] = comLayer;
		}

		return comLayers;
	}

	private CommunicationLayer[] prepareNettyNodes(String realmName, int[] viewProcessIds, int[] ports) {
		CommunicationLayer[] comLayers = new CommunicationLayer[viewProcessIds.length];
		for (int i = 0; i < comLayers.length; i++) {
			ReplicaTopology topology = CommunicationtTestMocker.mockTopologyWithTCP(viewProcessIds[i], viewProcessIds,
					ports);
			CommunicationLayer comLayer = new NettyServerCommunicationLayer(realmName, topology);
			comLayers[i] = comLayer;
		}

		return comLayers;
	}

	private MessageCounter[] prepareMessageCounters(CommunicationLayer[] comLayers) {
		MessageCounter[] counters = new MessageCounter[comLayers.length];
		for (int i = 0; i < counters.length; i++) {
			MessageCounter counter = new MessageCounter();
			comLayers[i].addMessageListener(SystemMessageType.CONSENSUS, counter);
			counters[i] = counter;
		}
		return counters;
	}

	private MessageCounter prepareMessageCounter(CommunicationLayer comLayer) {
		MessageCounter counter = new MessageCounter();
		comLayer.addMessageListener(SystemMessageType.CONSENSUS, counter);
		return counter;
	}

	private void broadcast(CommunicationLayer server, SystemMessage[] testMessages, int... targets) {
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
		public void onReceived(SystemMessage message) {
			messages.add(message);
		}

		public void clear() {
			messages.clear();
		}

		public void assertMessagesEquals(SystemMessage... expectedMessages) {
			SystemMessage[] actualMessages = messages.toArray(new SystemMessage[messages.size()]);
			assertEquals(expectedMessages.length, actualMessages.length);
			for (int i = 0; i < actualMessages.length; i++) {
				byte[] expected = BinarySerializeUtils.serialize(expectedMessages[i]);
				byte[] actual = BinarySerializeUtils.serialize(actualMessages[i]);
				assertArrayEquals(expected, actual);
			}
		}
	}

	private CommunicationLayer[] prepareNettyNodes2(String realmName, int[] viewProcessIds, int[] ports) {
		CommunicationLayer[] comLayers = new CommunicationLayer[viewProcessIds.length];
		for (int i = 0; i < comLayers.length; i++) {
			ReplicaTopology topology = CommunicationtTestMocker.mockTopologyWithTCP2(viewProcessIds[i], viewProcessIds, ports);
			CommunicationLayer comLayer = new NettyServerCommunicationLayer(realmName, topology);
			comLayers[i] = comLayer;
		}
		return comLayers;
	}

}
