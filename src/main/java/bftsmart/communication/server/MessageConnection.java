package bftsmart.communication.server;

import javax.crypto.SecretKey;

public interface MessageConnection {

	String MAC_ALGORITHM = "HmacMD5";

	SecretKey getSecretKey();

	/**
	 * Stop message sending and reception.
	 */
	void shutdown();

	/**
	 * Used to send packets to the remote server.
	 */
	/**
	 * @param data
	 * @param useMAC
	 * @param callback
	 * @return
	 */
	AsyncFuture<byte[], Void> send(byte[] data, boolean useMAC, CompletedCallback<byte[], Void> callback);

	/**
	 * Used to send packets to the remote server.
	 */
	/**
	 * @param data         要发送的数据；
	 * @param useMAC       是否使用 MAC；
	 * @param retrySending 当发送失败时，是否要重试；
	 * @param callback     发送完成回调；
	 * @return
	 * @throws InterruptedException
	 */
	AsyncFuture<byte[], Void> send(byte[] data, boolean useMAC, boolean retrySending,
			CompletedCallback<byte[], Void> callback);

}