package bftsmart.communication;

import javax.crypto.Mac;

import utils.io.BytesUtils;
import utils.io.NumberMask;
import utils.serialize.binary.BinarySerializeUtils;

public class SystemMessageCodec implements MessageCodec<SystemMessage> {

	private static final int MESSAGE_HEADER_SIZE = 4;
	
	private static final int MAC_HEADER_SIZE = 2;

	private boolean USE_MAC;
	private volatile Mac macSend;
	private volatile Mac macReceive;
	
	public SystemMessageCodec(boolean useMAC, String secretKeyString) {
		// TODO Auto-generated constructor stub
	}

	/**
	 * 编码系统消息；
	 * <p>
	 * 
	 * 输出的消息字节分为 4 个部分：<br>
	 * 1. 消息长度头：4个字节；<br>
	 * 2. 消息内容；<br>
	 * 3. MAC长度头：最多 2 个字节，采用 {@link NumberMask#SHORT} 格式输出；<br>
	 * 4. MAC内容；<br>
	 */
	@Override
	public byte[] encode(SystemMessage message) {
		byte[] messageBytes = BinarySerializeUtils.serialize(message);

		byte[] macBytes = BytesUtils.EMPTY_BYTES;
		if (USE_MAC) {
			macBytes = macSend.doFinal(messageBytes);
		}
		final int MESSAGE_SIZE = messageBytes.length;
		final int MAC_SIZE = macBytes.length;

		// do an extra copy of the data to be sent, but on a single out stream write
		byte[] outputBytes = new byte[MESSAGE_HEADER_SIZE + MESSAGE_SIZE + MAC_HEADER_SIZE + MAC_SIZE];

		// write message;
		BytesUtils.toBytes_BigEndian(MESSAGE_SIZE, outputBytes, 0);
		System.arraycopy(messageBytes, 0, outputBytes, MESSAGE_HEADER_SIZE, MESSAGE_SIZE);

		// write mac;
		BytesUtils.toBytes_BigEndian((short) macBytes.length, outputBytes, MESSAGE_HEADER_SIZE + MESSAGE_SIZE);
		if (macBytes.length > 0) {
			System.arraycopy(macBytes, 0, outputBytes, MESSAGE_HEADER_SIZE + MESSAGE_SIZE + MAC_HEADER_SIZE, MAC_SIZE);
		}
		return outputBytes;
	}

	@Override
	public synchronized SystemMessage decode(byte[] bytes)
			throws MessageAuthenticationException, IllegalMessageException {
		int messageSize = BytesUtils.toInt(bytes);
		if (messageSize < 0) {
			throw new IllegalMessageException("Illgal message bytes! Wrong header!");
		}
		
		int macSize = 0xFFFF & BytesUtils.toShort(bytes, MESSAGE_HEADER_SIZE + messageSize);
		if (macSize < 0) {
			throw new IllegalMessageException("Illgal message bytes! Wrong MAC header!");
		}

		// read mac;
		if (USE_MAC) {
			if (macSize == 0) {
				throw new MessageAuthenticationException("MAC is missing in the message!");
			}
			// 本地生成 MAC，验证消息；
			macReceive.update(bytes, MESSAGE_HEADER_SIZE, messageSize);
			byte[] expectedMacBytes = macReceive.doFinal();

			boolean macMatch = BytesUtils.equals(expectedMacBytes, 0, bytes, MESSAGE_HEADER_SIZE + messageSize + MAC_HEADER_SIZE, macSize);
			if (!macMatch) {
				throw new MessageAuthenticationException("Message authentication failed!");
			}
		}

		SystemMessage sm = BinarySerializeUtils.deserialize(bytes, MESSAGE_HEADER_SIZE, messageSize);
		sm.authenticated = USE_MAC;
		return sm;
	}


}
