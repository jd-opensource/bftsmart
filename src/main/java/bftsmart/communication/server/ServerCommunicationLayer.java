/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.communication.server;

import bftsmart.communication.MacKey;
import bftsmart.communication.MacMessageCodec;
import bftsmart.communication.SystemMessage;
import bftsmart.communication.queue.MessageQueue.SystemMessageType;

/**
 * 服务端节点通讯层；
 *
 */
public interface ServerCommunicationLayer {
	
	int getId();

	/**
	 * 返回指定远端节点关联的消息编解码器；
	 * 
	 * @param remoteId
	 * @return
	 */
	MacMessageCodec<SystemMessage> getMessageCodec(int remoteId);

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
