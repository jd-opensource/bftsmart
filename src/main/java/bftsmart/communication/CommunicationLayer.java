package bftsmart.communication;

import bftsmart.communication.MessageQueue.SystemMessageType;
import bftsmart.communication.impl.MessageListener;

/**
 * 服务端节点通讯层；
 *
 */
public interface CommunicationLayer {

	/**
	 * 当前节点的 Id；
	 * @return
	 */
	int getId();

	/**
	 * 返回指定远端节点关联的消息编解码器；
	 * 
	 * @param remoteId
	 * @return
	 */
	MacMessageCodec<SystemMessage> getMessageCodec(int remoteId);

	/**
	 * 返回当前节点与指定的节点的消息认证共享密钥；
	 * <p>
	 * 
	 * 如果与指定节点的连接尚未建立，则返回 null；
	 * 
	 * @param id
	 * @return
	 */
	default MacKey getMacKey(int id) {
		MacMessageCodec<SystemMessage> codec = getMessageCodec(id);
		return codec == null ? null : codec.getMacKey();
	}

	/**
	 * 根据最新的拓扑更新连接；
	 */
	void updateConnections();

	/**
	 * 发送消息；
	 * 
	 * @param targets
	 * @param sm
	 * @param useMAC
	 */
	default void send(int[] targets, SystemMessage sm) {
		send(targets, sm, true);
	}

	/**
	 * 发送消息；
	 * <p>
	 * 如果发送失败，会执行重试策略；
	 * 
	 * @param targets
	 * @param sm
	 * @param useMAC
	 */
	default void send(SystemMessage sm, int... targets) {
		send(targets, sm, true);
	}

	/**
	 * 发送消息；
	 * <p>
	 * 
	 * @param sm      消息；
	 * @param retry   是否重试；
	 * @param targets 接收目标；
	 */
	default void send(SystemMessage sm, boolean retry, int... targets) {
		send(targets, sm, retry);
	}

	/**
	 * 发送消息；
	 * 
	 * @param targets
	 * @param sm
	 * @param useMAC
	 * @param retrySending
	 */
	void send(int[] targets, SystemMessage sm, boolean retrySending);

	/**
	 * 设置消息接收监听器；
	 * 
	 * @param type
	 * @param listener
	 */
	void addMessageListener(SystemMessageType type, MessageListener listener);

	/**
	 * 启动通讯服务；
	 */
	void start();

	/**
	 * 关闭通讯服务；
	 */
	void close();

}
