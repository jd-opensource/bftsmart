package test.bftsmart.communication.server;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 基于流的消息连接；
 * 
 * @author huanghaiquan
 *
 */
public class MessageStreamNode {

	private final int id;

	private BlockingQueue<byte[]> inboundQueue = new LinkedBlockingQueue<byte[]>(1000);
	
	private Map<Integer, StreamPipeline> inboundPipelines = new ConcurrentHashMap<>();

	public MessageStreamNode(String realmName, int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	/**
	 * 返回当前消息节点的入站队列的流管道；
	 * 
	 * @return
	 */
	public synchronized StreamPipeline requestInboundPipeline(int fromId) {
		StreamPipeline pipeline = inboundPipelines.get(fromId);
		if (pipeline == null) {
			pipeline = new StreamPipeline(1000);
			inboundPipelines.put(fromId, pipeline);
		}
		return pipeline;
	}
}
