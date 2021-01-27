package bftsmart.communication;

import utils.io.BytesUtils;
import utils.io.NumberMask;
import utils.serialize.binary.BinarySerializeUtils;

public class SystemMessageCodec implements MessageCodec<SystemMessage> {

	private static final int MESSAGE_HEADER_SIZE = 4;

	private static final int MAC_HEADER_SIZE = 1;

	private boolean useMac;
	private volatile MacKey macKey;

	public SystemMessageCodec() {
	}

	public SystemMessageCodec(boolean useMAC, MacKey macKey) {
		setUseMac(useMAC);
		setMacKey(macKey);
	}

	public boolean isUseMac() {
		return useMac;
	}

	public MacKey getMacKey() {
		return macKey;
	}

	public void setMacKey(MacKey macKey) {
		if (macKey.getMacLength() > 255 || macKey.getMacLength() < 0) {
			throw new IllegalArgumentException("The MAC Length of specified MacKey is out of range[0 - 255]!");
		}
		this.macKey = macKey;
	}

	public void setUseMac(boolean useMac) {
		this.useMac = useMac;
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
		if (useMac) {
			macBytes = macKey.generateMac(messageBytes);
		}
		int messageSize = messageBytes.length;
		byte macSize = (byte) macBytes.length;

		// do an extra copy of the data to be sent, but on a single out stream write
		byte[] outputBytes = new byte[MESSAGE_HEADER_SIZE + messageSize + MAC_HEADER_SIZE + macSize];

		// write message;
		BytesUtils.toBytes_BigEndian(messageSize, outputBytes, 0);
		System.arraycopy(messageBytes, 0, outputBytes, MESSAGE_HEADER_SIZE, messageSize);

		// write mac;
		outputBytes[MESSAGE_HEADER_SIZE + messageSize] = macSize;
		if (macBytes.length > 0) {
			System.arraycopy(macBytes, 0, outputBytes, MESSAGE_HEADER_SIZE + messageSize + MAC_HEADER_SIZE, macSize);
		}
		return outputBytes;
	}

	@Override
	public synchronized SystemMessage decode(byte[] encodedMessageBytes)
			throws MessageAuthenticationException, IllegalMessageException {
		int messageSize = BytesUtils.toInt(encodedMessageBytes);
		if (messageSize < 0) {
			throw new IllegalMessageException("Illgal encoded message bytes! Wrong message header!");
		}
		if (encodedMessageBytes.length < MESSAGE_HEADER_SIZE + messageSize + MAC_HEADER_SIZE) {
			throw new IllegalMessageException("Too short length of encoded message bytes!");
		}

		int macSize = 0xFF & encodedMessageBytes[MESSAGE_HEADER_SIZE + messageSize];
		if (macSize < 0) {
			throw new IllegalMessageException("Illgal encoded message bytes! Wrong mac header!");
		}

		// read mac;
		if (useMac) {
			if (macSize == 0) {
				throw new MessageAuthenticationException("The MAC is missing in the received message!");
			}
			if (encodedMessageBytes.length < MESSAGE_HEADER_SIZE + messageSize + MAC_HEADER_SIZE + macSize) {
				throw new IllegalMessageException("Too short length of encoded message bytes!");
			}
			// 本地生成 MAC，验证消息；
			boolean macMatch = macKey.authenticate(encodedMessageBytes, MESSAGE_HEADER_SIZE, messageSize,
					encodedMessageBytes, MESSAGE_HEADER_SIZE + messageSize + MAC_HEADER_SIZE);
			if (!macMatch) {
				throw new MessageAuthenticationException("Message authentication failed!");
			}
		}

		SystemMessage sm = BinarySerializeUtils.deserialize(encodedMessageBytes, MESSAGE_HEADER_SIZE, messageSize);
		sm.authenticated = useMac;
		return sm;
	}

}
