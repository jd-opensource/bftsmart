package bftsmart.communication.server;

import java.io.Closeable;

import bftsmart.communication.MacMessageCodec;
import bftsmart.communication.SystemMessage;

/**
 * 面向消息的连接；
 * 
 * @author huanghaiquan
 *
 */
public interface MessageConnection extends Closeable {

	/**
	 * 连接的远端节点 ID；
	 * 
	 * @return
	 */
	int getRemoteId();

	/**
	 * 连接是否还在存活；
	 * 
	 * @return
	 */
	boolean isAlived();

	/**
	 * 当前连接关联的消息编解码器；
	 * 
	 * @return
	 */
	MacMessageCodec<SystemMessage> getMessageCodec();

	/**
	 * 开始连接的数据处理；
	 */
	void start();

	/**
	 * 关闭连接；
	 * <p>
	 * 
	 * 连接关闭后将不再处理数据；
	 */
	@Override
	void close();

	/**
	 * 清除发送队列中尚未发送的数据；
	 */
	void clearSendingQueue();
	
	/**
	 * 发送消息；
	 * 
	 * @param data         要发送的数据；
	 * @param useMAC       是否使用 MAC；
	 * @param retrySending 当发送失败时，是否要重试；
	 * @param callback     发送完成回调；
	 * @return
	 */
	AsyncFuture<SystemMessage, Void> send(SystemMessage message, boolean retrySending,
			CompletedCallback<SystemMessage, Void> callback);

}