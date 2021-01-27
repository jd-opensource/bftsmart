package test.bftsmart.communication;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.security.PublicKey;
import java.util.Properties;

import org.junit.Test;

import bftsmart.communication.DHPubKeyCertificate;
import bftsmart.communication.IllegalMessageException;
import bftsmart.communication.MacKey;
import bftsmart.communication.MacKeyGenerator;
import bftsmart.communication.MessageAuthenticationException;
import bftsmart.communication.SystemMessageCodec;
import bftsmart.consensus.messages.ConsensusMessage;
import bftsmart.consensus.messages.MessageFactory;
import bftsmart.reconfiguration.util.HostsConfig;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.tom.ReplicaConfiguration;
import utils.security.RandomUtils;

public class SystemMessageCodecTest {

	@Test
	public void testEncode() throws MessageAuthenticationException, IllegalMessageException {

		// 初始化 MAC 密钥生成器；
		final int[] viewProcessIds = { 0, 1, 2, 3 };

		ReplicaConfiguration conf0 = generateConfig(0, viewProcessIds);
		PublicKey pubKey0 = conf0.getRSAPublicKey(0);
		MacKeyGenerator macKeyGen0 = new MacKeyGenerator(pubKey0, conf0.getRSAPrivateKey(), conf0.getDHG(),
				conf0.getDHP());

		ReplicaConfiguration conf1 = generateConfig(1, viewProcessIds);
		PublicKey pubKey1 = conf0.getRSAPublicKey(0);
		MacKeyGenerator macKeyGen1 = new MacKeyGenerator(pubKey1, conf1.getRSAPrivateKey(), conf1.getDHG(),
				conf1.getDHP());

		/**
		 * 进行密钥交互，生成共享密钥；
		 */
		DHPubKeyCertificate dhPubKeyCert0 = macKeyGen0.getDHPubKeyCertificate();
		DHPubKeyCertificate dhPubKeyCert1 = macKeyGen1.getDHPubKeyCertificate();

		MacKey macKey_0_to_1 = macKeyGen0.exchange(dhPubKeyCert1);
		MacKey macKey_1_to_0 = macKeyGen1.exchange(dhPubKeyCert0);

		// 测试消息编解码器的编解码一致性；
		SystemMessageCodec messageCodec0 = new SystemMessageCodec(true, macKey_0_to_1);
		SystemMessageCodec messageCodec1 = new SystemMessageCodec(true, macKey_1_to_0);

		ConsensusMessage message0 = createTestMessage(0);

		byte[] encodedBytesOfNode0 = messageCodec0.encode(message0);
		ConsensusMessage decodedMessageOfNode1 = (ConsensusMessage)messageCodec1.decode(encodedBytesOfNode0);
		assertMessageEquals(message0, decodedMessageOfNode1);
		
		ConsensusMessage message1 = createTestMessage(1);
		byte[] encodedBytesOfNode1 = messageCodec1.encode(message1);
		ConsensusMessage decodedMessageOfNode0 = (ConsensusMessage)messageCodec0.decode(encodedBytesOfNode1);
		assertMessageEquals(message1, decodedMessageOfNode0);
	}
	
	private void assertMessageEquals(ConsensusMessage expecedMessage, ConsensusMessage actualMessage) {
		assertEquals(expecedMessage.getNumber(), actualMessage.getNumber());
		assertEquals(expecedMessage.getEpoch(), actualMessage.getEpoch());
		assertEquals(expecedMessage.getSender(), actualMessage.getSender());
		assertArrayEquals(expecedMessage.getValue(), actualMessage.getValue());
	}

	private ConsensusMessage createTestMessage(int senderId) {
		MessageFactory messageFactory = new MessageFactory(senderId);
		return messageFactory.createPropose(0, 0, RandomUtils.generateRandomBytes(100));
	}

	private static ReplicaConfiguration generateConfig(int id, int[] viewProcessIds) {
		HostsConfig hosts = new HostsConfig();
		hosts.add(0, "localhost", 10010, 10012);
		hosts.add(1, "localhost", 10020, 10022);
		hosts.add(2, "localhost", 10030, 10032);
		hosts.add(3, "localhost", 10040, 10042);

		TOMConfiguration conf = new TOMConfiguration(0, new Properties(), hosts);
		return conf;
	}
}
