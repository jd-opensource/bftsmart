package test.bftsmart.communication.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import bftsmart.communication.SystemMessage;
import bftsmart.communication.queue.MessageInQueue;
import bftsmart.communication.queue.MessageQueue.SystemMessageType;
import bftsmart.communication.server.AbstractServersCommunicationLayer;
import bftsmart.communication.server.MessageConnection;
import bftsmart.communication.server.MessageQueueConnection;
import bftsmart.communication.server.ServerCommunicationLayer;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.ViewTopology;
import utils.security.RandomUtils;

public class ServerCommunicationLayerTest {

	private static Logger LOGGER = LoggerFactory.getLogger(ServerCommunicationLayerTest.class);

	private static final MetricRegistry metrics = new MetricRegistry();

	private static Meter requests = metrics.meter("requests");

	private static void startReport() {
		ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)//
				.convertRatesTo(TimeUnit.SECONDS)//
				.convertDurationsTo(TimeUnit.MILLISECONDS)//
				.build();

		reporter.start(3, TimeUnit.SECONDS);
	}

	@Test
	public void test() {
		startReport();

		requests.mark();
		LOGGER.debug("START...");

		final String realmName = "TEST-NET";
		int[] viewProcessIds = { 0, 1, 2, 3 };
		final MessageFactory messageFactory0 = new MessageFactory(0);
		final MessageFactory messageFactory1 = new MessageFactory(1);
		final MessageFactory messageFactory2 = new MessageFactory(2);
		final MessageFactory messageFactory3 = new MessageFactory(3);

		MessageNetwork messageNetwork = new MessageNetwork(viewProcessIds);

		ViewTopology topology0 = mockTopology(0, viewProcessIds);
		ServerCommunicationLayer server0 = new MessageQueueCommunicationLayer(realmName, topology0, messageNetwork);

		ViewTopology topology1 = mockTopology(1, viewProcessIds);
		ServerCommunicationLayer server1 = new MessageQueueCommunicationLayer(realmName, topology1, messageNetwork);

		ViewTopology topology2 = mockTopology(2, viewProcessIds);
		ServerCommunicationLayer server2 = new MessageQueueCommunicationLayer(realmName, topology2, messageNetwork);

		ViewTopology topology3 = mockTopology(3, viewProcessIds);
		ServerCommunicationLayer server3 = new MessageQueueCommunicationLayer(realmName, topology3, messageNetwork);

		requests.mark();

		// 生成待发送的测试消息；
		SystemMessage[] testMessages = prepareMessages(messageFactory0, 6);

		requests.mark();
		// 从 server0 广播给全部的节点，包括 server0 自己；
		broadcast(server0, viewProcessIds, testMessages);

		requests.mark();
		// 验证所有节点都能完整地收到消息；
		assertMessageConsuming(server0, testMessages);
		assertMessageConsuming(server1, testMessages);
		assertMessageConsuming(server2, testMessages);
		assertMessageConsuming(server3, testMessages);

		requests.mark();

		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
		}
	}

	private void assertMessageConsuming(ServerCommunicationLayer server, SystemMessage[] testMessages) {
		List<SystemMessage> receivedMessages = new ArrayList<>();
		SystemMessage msg = null;
		while ((msg = server.consume(SystemMessageType.CONSENSUS, 100, TimeUnit.MILLISECONDS)) != null) {
			receivedMessages.add(msg);
		}
		assertTrue(receivedMessages.size() >= testMessages.length);
		for (int i = 0; i < testMessages.length; i++) {
			SystemMessage recvMsg = receivedMessages.get(i);
			assertEquals(testMessages[i], recvMsg);
		}
	}

	private void broadcast(ServerCommunicationLayer server, int[] targets, SystemMessage[] testMessages) {
		for (int i = 0; i < testMessages.length; i++) {
			server.send(targets, testMessages[i], false);
		}
	}

	private SystemMessage[] prepareMessages(MessageFactory messageFactory, int count) {
		SystemMessage[] messages = new SystemMessage[count];
		for (int i = 0; i < count; i++) {
			ConsensusMessage msg = messageFactory.createPropose(0, i, RandomUtils.generateRandomBytes(20));
			messages[i] = msg;
		}
		return messages;
	}

	private ViewTopology mockTopology(int currentId, int[] processIds) {
		ViewTopology topology = Mockito.mock(ViewTopology.class);
		when(topology.getCurrentProcessId()).thenReturn(currentId);
		when(topology.getCurrentViewProcesses()).thenReturn(processIds);

		return topology;
	}

	private static class MessageNetwork {

		private int[] processIds;

		private MessageInQueue[] messageQueues;

		public MessageNetwork(int[] processIds) {
			this.processIds = processIds.clone();
			this.messageQueues = new MessageInQueue[processIds.length];
			for (int i = 0; i < processIds.length; i++) {
				messageQueues[i] = new MessageInQueue(Integer.MAX_VALUE);
			}
		}

		public MessageInQueue getQueueOfNode(int processId) {
			int i = 0;
			for (int procId : processIds) {
				if (processId == procId) {
					return messageQueues[i];
				}
				i++;
			}
			throw new IllegalArgumentException(
					"The specified id[" + processId + "] is out of range" + Arrays.toString(processIds) + "! ");
		}
	}

	/**
	 * 基于队列对消息直接投递的通讯层实现；
	 * 
	 * @author huanghaiquan
	 *
	 */
	private static class MessageQueueCommunicationLayer extends AbstractServersCommunicationLayer {

		private MessageNetwork messageNetwork;

		public MessageQueueCommunicationLayer(String realmName, ViewTopology topology, MessageNetwork messageNetwork) {
			super(realmName, topology, messageNetwork.getQueueOfNode(topology.getCurrentProcessId()));
			this.messageNetwork = messageNetwork;
		}

		@Override
		protected void startCommunicationServer() {
		}

		@Override
		protected void closeCommunicationServer() {
		}

		@Override
		protected MessageConnection connectRemote(int remoteId) {
			MessageInQueue queue = messageNetwork.getQueueOfNode(remoteId);
			return new MessageQueueConnection(realmName, remoteId, queue);
		}

		@Override
		protected MessageConnection acceptRemote(int remoteId) {
			MessageInQueue queue = messageNetwork.getQueueOfNode(remoteId);
			return new MessageQueueConnection(realmName, remoteId, queue);
		}

	}

}
