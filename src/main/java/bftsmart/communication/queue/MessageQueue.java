package bftsmart.communication.queue;

import bftsmart.communication.SystemMessage;

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
	boolean offer(MSG_TYPE type, SystemMessage sm);

	/**
	 * 将消息放入队列中
	 *
	 * @param type 消息类型
	 * @param sm   消息对象
	 */
	void put(MSG_TYPE type, SystemMessage sm) throws InterruptedException;

	/**
	 * 从队列中获取指定类型消息
	 * 
	 * @param type    消息类型
	 * @param timeout 超时时间
	 * @param unit    超时时间单位
	 * @return
	 */
	SystemMessage poll(MSG_TYPE type, long timeout, TimeUnit unit) throws InterruptedException;

	enum QUEUE_TYPE {
		/**
		 * Socket消息接收队列
		 */
		IN,
		/**
		 * Socket消息发送队列
		 */
		OUT,;
	}

	enum MSG_TYPE {
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
	}
}
