package bftsmart.communication.queue;

import bftsmart.communication.SystemMessage;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.tom.leaderchange.HeartBeatMessage;
import bftsmart.tom.leaderchange.LeaderRequestMessage;
import bftsmart.tom.leaderchange.LeaderResponseMessage;
import bftsmart.tom.leaderchange.LeaderStatusRequestMessage;
import bftsmart.tom.leaderchange.LeaderStatusResponseMessage;

import java.util.concurrent.TimeUnit;

public interface MessageQueue {

	/**
	 * 将消息放入队列
	 *
	 * @param type 消息类型
	 * @param sm   消息对象
	 * @return 是否放入成功
	 * @throws InterruptedException
	 */
	boolean offer(SystemMessageType type, SystemMessage sm);

	/**
	 * 将消息放入队列中
	 *
	 * @param type 消息类型
	 * @param sm   消息对象
	 */
	void put(SystemMessageType type, SystemMessage sm) throws InterruptedException;

	/**
	 * 从队列中获取指定类型消息
	 * 
	 * @param type    消息类型
	 * @param timeout 超时时间
	 * @param unit    超时时间单位
	 * @return
	 */
	SystemMessage poll(SystemMessageType type, long timeout, TimeUnit unit) throws InterruptedException;

	/**
	 * 从队列中获取指定类型消息； 如果没有消息，则堵塞等待，直到有消息返回；
	 * 
	 * @param type    消息类型
	 * @return
	 */
	SystemMessage take(SystemMessageType type) throws InterruptedException;

	public static enum QueueDirection {
		/**
		 * Socket消息接收队列
		 */
		IN,
		/**
		 * Socket消息发送队列
		 */
		OUT,;
	}

	public static enum SystemMessageType {
		/**
		 * 共识消息
		 */
		CONSENSUS,
		/**
		 * 心跳相关消息
		 */
		HEART,
		/**
		 * 领导者改变相关消息
		 */
		LC;

		/**
		 * 返回消息类型枚举
		 *
		 * @param sm 消息
		 * @return 枚举类型
		 */
		public static MessageQueue.SystemMessageType typeOf(SystemMessage sm) {
			if (sm instanceof ConsensusMessage) {
				return MessageQueue.SystemMessageType.CONSENSUS;
			} else if (sm instanceof HeartBeatMessage || sm instanceof LeaderRequestMessage
					|| sm instanceof LeaderResponseMessage || sm instanceof LeaderStatusRequestMessage
					|| sm instanceof LeaderStatusResponseMessage) {
				return MessageQueue.SystemMessageType.HEART;
			} else {
				return MessageQueue.SystemMessageType.LC;
			}
		}
	}
}
