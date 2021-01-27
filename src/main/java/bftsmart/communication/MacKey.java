package bftsmart.communication;

import javax.crypto.SecretKey;

public interface MacKey {

	SecretKey getSecretKey();

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
	 * @param message
	 * @param mac
	 * @return
	 */
	boolean authenticate(byte[] message, byte[] mac);

}
