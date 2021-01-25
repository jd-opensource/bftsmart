package test.bftsmart.communication.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import bftsmart.communication.queue.MessageQueue;

class MessageQueuesNetwork {

		private Map<Integer, MessageQueue> queues = new ConcurrentHashMap<Integer, MessageQueue>();

		public MessageQueue getQueueOfNode(int processId) {
			MessageQueue queue = queues.get(processId);
			if (queue == null) {
				throw new IllegalArgumentException("The specified id[" + processId + "] is not registered! ");
			}
			return queue;
		}

		public void register(int processId, MessageQueue queue) {
			queues.put(processId, queue);
		}
	}