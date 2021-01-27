package test.bftsmart.communication;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.PublicKey;
import java.util.Properties;

import org.junit.Test;

import bftsmart.communication.DHPubKeyCertificate;
import bftsmart.communication.MacKeyGenerator;
import bftsmart.communication.MacKey;
import bftsmart.reconfiguration.util.HostsConfig;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.tom.ReplicaConfiguration;
import utils.security.RandomUtils;

public class MACKeyGeneratorTest {

	@Test
	public void testExchange() {
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

		// 测试 MAC 共享密钥证书的编码、签署和验证；
		byte[] encodedDHPubKeyCertBytes0 = dhPubKeyCert0.getEncodedBytes();
		byte[] encodedDHPubKeyCertBytes1 = dhPubKeyCert1.getEncodedBytes();

		// 解析和验证密钥认证；
		DHPubKeyCertificate verifedDhPubKeyCert0 = MacKeyGenerator.resolveAndVerify(encodedDHPubKeyCertBytes0, pubKey0);
		DHPubKeyCertificate verifedDhPubKeyCert1 = MacKeyGenerator.resolveAndVerify(encodedDHPubKeyCertBytes1, pubKey1);

		// 密钥交互；
		MacKey macKey_0_to_1 = macKeyGen0.exchange(verifedDhPubKeyCert1);
		MacKey macKey_1_to_0 = macKeyGen1.exchange(verifedDhPubKeyCert0);
		
		// 验证两个生成的密钥是一致的；
		assertArrayEquals(macKey_0_to_1.getSecretKey().getEncoded(), macKey_1_to_0.getSecretKey().getEncoded());

		// 根据共享密钥生成消息认证码；
		// 测试双方的相互认证；
		byte[] message = RandomUtils.generateRandomBytes(100);

		byte[] mac0 = macKey_0_to_1.generateMac(message);
		assertEquals(macKey_0_to_1.getMacLength(), mac0.length);
		boolean authBy1 = macKey_1_to_0.authenticate(message, mac0);
		assertTrue(authBy1);

		byte[] mac1 = macKey_1_to_0.generateMac(message);
		assertEquals(macKey_1_to_0.getMacLength(), mac1.length);
		boolean authBy0 = macKey_0_to_1.authenticate(message, mac1);
		assertTrue(authBy0);

		assertArrayEquals(mac0, mac1);

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
