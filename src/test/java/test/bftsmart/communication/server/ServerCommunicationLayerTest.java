package test.bftsmart.communication.server;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.communication.SystemMessage;
import bftsmart.communication.queue.MessageQueue.SystemMessageType;
import bftsmart.communication.server.MessageListener;
import bftsmart.communication.server.ServerCommunicationLayer;
import bftsmart.communication.server.socket.SocketServerCommunicationLayer;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.ReplicaTopology;
import bftsmart.reconfiguration.ViewTopology;
import utils.security.RandomUtils;

public class ServerCommunicationLayerTest {

	private static Logger LOGGER = LoggerFactory.getLogger(ServerCommunicationLayerTest.class);

//	private static final MetricRegistry metrics = new MetricRegistry();
//
//	private static Meter requests = metrics.meter("requests");
//
//	private static void startReport() {
//		ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)//
//				.convertRatesTo(TimeUnit.SECONDS)//
//				.convertDurationsTo(TimeUnit.MILLISECONDS)//
//				.build();
//
//		reporter.start(3, TimeUnit.SECONDS);
//	}

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
		broadcast(servers[0], viewProcessIds, testMessages);

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
	public void testStreamNodes() {
		final String realmName = "TEST-NET";
		int[] viewProcessIds = { 0, 1, 2, 3 };
		
		final MessageStreamNodeNetwork messageNetwork = new MessageStreamNodeNetwork();
		
		ServerCommunicationLayer[] servers = prepareStreamNodes(realmName, viewProcessIds, messageNetwork);
		MessageCounter[] counters = prepareMessageCounters(servers);
		
		for (ServerCommunicationLayer srv : servers) {
			srv.start();
		}
		
		// 生成待发送的测试消息；
		SystemMessage[] testMessages = prepareMessages(0, 6);
		
		// 从 server0 广播给全部的节点，包括 server0 自己；
		broadcast(servers[0], viewProcessIds, testMessages);
		
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
	public void testSocketNodes() {
		final String realmName = "TEST-NET";
		int[] viewProcessIds = { 0, 1, 2, 3 };
		int[] ports = {15100, 15110, 15120, 15130};
		
		ServerCommunicationLayer[] servers = prepareSocketNodes(realmName, viewProcessIds, ports);
		MessageCounter[] counters = prepareMessageCounters(servers);
		
		for (ServerCommunicationLayer srv : servers) {
			srv.start();
		}
		
		// 生成待发送的测试消息；
		SystemMessage[] testMessages = prepareMessages(0, 6);
		
		// 从 server0 广播给全部的节点，包括 server0 自己；
		broadcast(servers[0], viewProcessIds, testMessages);
		
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
	
	private ServerCommunicationLayer[] prepareQueueNodes(String realmName, int[] viewProcessIds, MessageQueuesNetwork messageNetwork) {
		ServerCommunicationLayer[] comLayers = new ServerCommunicationLayer[viewProcessIds.length];
		for (int i = 0; i < comLayers.length; i++) {
			ViewTopology topology = CommunicationtTestMocker.mockTopology(viewProcessIds[i], viewProcessIds);
			ServerCommunicationLayer comLayer = new MessageQueueCommunicationLayer(realmName, topology, messageNetwork);
			comLayers[i] = comLayer;
		}
		
		return comLayers;
	}
	
	private ServerCommunicationLayer[] prepareStreamNodes(String realmName, int[] viewProcessIds, MessageStreamNodeNetwork messageNetwork) {
		ServerCommunicationLayer[] comLayers = new ServerCommunicationLayer[viewProcessIds.length];
		for (int i = 0; i < comLayers.length; i++) {
			ViewTopology topology = CommunicationtTestMocker.mockTopology(viewProcessIds[i], viewProcessIds);
			ServerCommunicationLayer comLayer = new MessageStreamCommunicationLayer(realmName, topology, messageNetwork);
			comLayers[i] = comLayer;
		}
		
		return comLayers;
	}
	
	private ServerCommunicationLayer[] prepareSocketNodes(String realmName, int[] viewProcessIds, int[] ports) {
		ServerCommunicationLayer[] comLayers = new ServerCommunicationLayer[viewProcessIds.length];
		for (int i = 0; i < comLayers.length; i++) {
			ReplicaTopology topology = CommunicationtTestMocker.mockTopologyWithTCP(viewProcessIds[i], viewProcessIds, ports);
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

	private void broadcast(ServerCommunicationLayer server, int[] targets, SystemMessage[] testMessages) {
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
