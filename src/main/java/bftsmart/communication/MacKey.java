package bftsmart.communication;

import javax.crypto.SecretKey;

public interface MacKey {

	SecretKey getSecretKey();

	/**
	 * 消息认证码（MAC）的长度；
	 * 
	 * @return
	 */
	int getMacLength();

	/**
	 * 生成消息认证码（MAC）；
	 * 
	 * @param message
	 * @return
	 */
	byte[] generateMac(byte[] message);

	/**
	 * 认证指定的消息与认证码是否一致；
	 * 
	 * @param message 消息数据；
	 * @param mac
	 * @return
	 */
	boolean authenticate(byte[] message, byte[] mac);

	/**
	 * 认证指定的消息与认证码是否一致；
	 * 
	 * @param message       消息数据；
	 * @param messageOffset 消息内容的起始偏移量
	 * @param messageSize   消息内容的长度；
	 * @param mac           消息认证码数据；
	 * @param macOffset     消息认证码的起始偏移量；
	 * @return
	 */
	boolean authenticate(byte[] message, int messageOffset, int messageSize, byte[] mac, int macOffset);
}
