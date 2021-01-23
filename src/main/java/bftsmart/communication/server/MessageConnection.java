package bftsmart.communication.server;

import javax.crypto.SecretKey;

import bftsmart.communication.SystemMessage;

public interface MessageConnection {

//	public static final String MAC_ALGORITHM = "HmacMD5";
//	public static final String SECRET_KEY_ALGORITHM = "PBEWithMD5AndDES";
	
	public static final String MAC_ALGORITHM = "HmacSHA256";

	public static final String SECRET_KEY_ALGORITHM = "PBEWithHmacSHA256AndAES_128";

	SecretKey getSecretKey();

	int getRemoteId();

	boolean isAlived();

	/**
	 * 
	 */
	void start();

	/**
	 * 关闭连接；
	 * <p>
	 * 
	 * 连接关闭后将不再处理数据；
	 */
	void shutdown();

	/**
	 * 清除发送队列中尚未发送的数据；
	 */
	void clearOutQueue();

	/**
	 * 发送消息；
	 * 
	 * @param data         要发送的数据；
	 * @param useMAC       是否使用 MAC；
	 * @param retrySending 当发送失败时，是否要重试；
	 * @param callback     发送完成回调；
	 * @return
	 */
	AsyncFuture<SystemMessage, Void> send(SystemMessage message, boolean useMAC, boolean retrySending,
			CompletedCallback<SystemMessage, Void> callback);

}