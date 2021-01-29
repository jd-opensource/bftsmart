package bftsmart.communication.impl.queue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import bftsmart.communication.MessageQueue;

/**
 * 消息队列管理器；
 * 
 * @author huanghaiquan
 *
 */
public class MessageQueueManager {

	private Map<Integer, MessageQueue> queues = new ConcurrentHashMap<Integer, MessageQueue>();

	public MessageQueue getQueue(int processId) {
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