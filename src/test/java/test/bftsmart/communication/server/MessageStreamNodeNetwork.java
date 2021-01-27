package test.bftsmart.communication.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import bftsmart.communication.queue.MessageQueue;
import bftsmart.communication.server.MessageConnection;

class MessageStreamNodeNetwork {

		private Map<Integer, MessageStreamNode> nodes = new ConcurrentHashMap<Integer, MessageStreamNode>();
		
		
		public void register(MessageStreamNode node) {
			nodes.put(node.getId(), node);
		}

		public MessageStreamNode getNode(int processId) {
			MessageStreamNode conn = nodes.get(processId);
			if (conn == null) {
				throw new IllegalArgumentException("The specified id[" + processId + "] is not exist! ");
			}
			return conn;
		}

	}