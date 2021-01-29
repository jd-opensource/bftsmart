package bftsmart.communication;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PublicKey;

import bftsmart.communication.impl.IOChannel;
import utils.io.BytesUtils;

/**
 * 通道认证器；
 * <p>
 * 
 * 认证通过后将产生一个用于通讯的共享密钥 {@link MacKey} ；
 * 
 * @author huanghaiquan
 *
 */
public class MacAuthenticator {

	private final MacKeyGenerator MAC_KEY_GEN;

	private final int REMOTE_ID;

	private PublicKey remotePubKey;

	private MacKey macKey;

	/**
	 * @param remoteId
	 * @param remotePubKey
	 * @param generator
	 * @param in
	 * @param out
	 * @throws MacAuthenticationException
	 * @throws IOException
	 */
	public MacAuthenticator(int remoteId, PublicKey remotePubKey, MacKeyGenerator generator)  {
		this.MAC_KEY_GEN = generator;
		this.REMOTE_ID = remoteId;
		this.remotePubKey = remotePubKey;
	}

	public MacKey getMacKey() {
		return macKey;
	}
	
	/**
	 * 通过指定的输入输出通道完成一次双向的认证；
	 * 
	 * @param channel
	 * @return
	 * @throws MacAuthenticationException
	 * @throws IOException
	 */
	public MacKey authenticate(IOChannel channel) throws MacAuthenticationException, IOException {
		return authenticate(channel.getOutputStream(), channel.getInputStream());
	}
	

	/**
	 * 通过指定的输入输出流完成一次双向的认证；
	 * 
	 * <br>
	 * 
	 * @param out
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public MacKey authenticate(OutputStream out, InputStream in) throws MacAuthenticationException, IOException {
		// 发送 DH key；
		DHPubKeyCertificate currentDHPubKeyCert = MAC_KEY_GEN.getDHPubKeyCertificate();
		sendDHKey(out, currentDHPubKeyCert);

		// 接收 DH key;
		DHPubKeyCertificate remoteDHPubKeyCert = receiveDHKey(remotePubKey, in);
		if (remoteDHPubKeyCert == null) {
			// 认证失败；
			throw new MacAuthenticationException(String.format(
					"The DHPubKey verification failed while establishing connection with remote[%s]!", REMOTE_ID));
		}

		// 生成共享密钥
		MacKey macKey = MAC_KEY_GEN.exchange(remoteDHPubKeyCert);

		return macKey;
	}

	private static void sendDHKey(OutputStream out, DHPubKeyCertificate currentDHPubKeyCert) throws IOException {
		byte[] encodedBytes = currentDHPubKeyCert.getEncodedBytes();

		// send my DH public key and signature
		BytesUtils.writeInt(encodedBytes.length, out);
		out.write(encodedBytes);
	}

	/**
	 * 接收和验证“密钥交互公钥凭证”；
	 * <p>
	 * 如果验证失败，则返回 null;
	 * 
	 * @param identityKey 用于验证接收数据中的签名身份的公钥；
	 * @param in          输入流；
	 * @return 密钥交换凭证；
	 * @throws IOException 发生 IO 异常；
	 */
	private static DHPubKeyCertificate receiveDHKey(PublicKey identityKey, InputStream in) throws IOException {
		// receive remote DH public key and signature
		int remoteMacPubKeyCertLength = BytesUtils.readInt(in);
		byte[] remoteMacPubKeyCertBytes = new byte[remoteMacPubKeyCertLength];
		int read = 0;
		do {
			read += in.read(remoteMacPubKeyCertBytes, read, remoteMacPubKeyCertLength - read);

		} while (read < remoteMacPubKeyCertLength);

		return MacKeyGenerator.resolveAndVerify(remoteMacPubKeyCertBytes, identityKey);
	}

}