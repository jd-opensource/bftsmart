package bftsmart.communication;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.bouncycastle.util.Arrays;

import utils.io.BytesUtils;

public class MACKeyGenerator {

//	public static final String MAC_ALGORITHM = "HmacMD5";
//	public static final String SECRET_KEY_ALGORITHM = "PBEWithMD5AndDES";

	public static final String MAC_ALGORITHM = "HmacSHA256";

	public static final String SECRET_KEY_ALGORITHM = "PBEWithHmacSHA256AndAES_128";

	private final PrivateKey rsaPrivKey;

	@SuppressWarnings("unused")
	private final BigInteger dhG;

	private final BigInteger dhP;

	private final BigInteger dhPubKey;

	private final DHPubKeyCertificate dhPubKeyCert;

	/**
	 * MAC 密钥生成器；
	 * 
	 * @param rasPubKey
	 * @param rsaPrivKey
	 * @param dhG
	 * @param dhP
	 */
	public MACKeyGenerator(PublicKey rasPubKey, PrivateKey rsaPrivKey, BigInteger dhG, BigInteger dhP) {
		this.rsaPrivKey = rsaPrivKey;
		this.dhG = dhG;
		this.dhP = dhP;

		this.dhPubKey = generateDHPubKey(dhG, dhP, rsaPrivKey);
		this.dhPubKeyCert = signKey(dhPubKey, rsaPrivKey);
	}

	public BigInteger getDHPubKey() {
		return dhPubKey;
	}

	/**
	 * 生成和指定方的共享密钥；
	 * 
	 * @param partiDHPubKey 共享密钥的对手方公钥；
	 * @return
	 * @throws InvalidKeySpecException
	 * @throws InvalidKeyException
	 */
	public MacKey exchange(DHPubKeyCertificate partiDHPubKeyCert) {
		BigInteger dhPrivKey = new BigInteger(rsaPrivKey.getEncoded());
		BigInteger secretKey = partiDHPubKeyCert.getDHPubKey().modPow(dhPrivKey, dhP);

		try {
			SecretKeyFactory fac = SecretKeyFactory.getInstance(SECRET_KEY_ALGORITHM);
			PBEKeySpec spec = new PBEKeySpec(secretKey.toString().toCharArray());
			SecretKey authKey = fac.generateSecret(spec);

			MacKeyEntry macKey = new MacKeyEntry(authKey);
			return macKey;
		} catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	/**
	 * 自签名生成的公钥和签名数据；
	 * 
	 * @return
	 */
	public DHPubKeyCertificate getDHPubKeyCertificate() {
		return dhPubKeyCert;
	}

	/**
	 * 解析并验证指定的“密钥交互公钥凭证”是否是由指定公钥代表的身份进行签发的；
	 * 
	 * @param encodedDHPubKeyCertBytes “密钥交互公钥凭证”的字节数组；
	 * @param pubKey                   签发者的公钥；
	 * @return 返回解析并验证通过的“密钥交互公钥凭证”实例，如果签名不匹配，则返回 null；
	 */
	public static DHPubKeyCertificate resolveAndVerify(byte[] encodedDHPubKeyCertBytes, PublicKey pubKey) {
		int dhPubKeyLength = BytesUtils.toInt(encodedDHPubKeyCertBytes);
		if (dhPubKeyLength <= 0) {
			throw new IllegalAccessError("Illegal encoded DHPubKey certificate bytes!");
		}
		int signatureLength = BytesUtils.toInt(encodedDHPubKeyCertBytes, 4 + dhPubKeyLength);

		byte[] dhPubKeyBytes = Arrays.copyOfRange(encodedDHPubKeyCertBytes, 4, 4 + dhPubKeyLength);
		BigInteger dhPubKey = new BigInteger(dhPubKeyBytes);

		byte[] signatureBytes = Arrays.copyOfRange(encodedDHPubKeyCertBytes, 4 + dhPubKeyLength + 4,
				4 + dhPubKeyLength + 4 + signatureLength);

		boolean verified = verify(pubKey, dhPubKeyBytes, signatureBytes);
		if (!verified) {
			return null;
		}
		return new DHPubKeyCertEntry(dhPubKey, signatureBytes);
	}

	/**
	 * 签署密钥交换的公钥；
	 * 
	 * @param dhPubKey
	 * @param rsaPrivKey
	 * @return
	 */
	private static DHPubKeyCertEntry signKey(BigInteger dhPubKey, PrivateKey rsaPrivKey) {
		byte[] dhPubKeyBytes = dhPubKey.toByteArray();
		byte[] signatureBytes = sign(rsaPrivKey, dhPubKeyBytes);
		return new DHPubKeyCertEntry(dhPubKey, signatureBytes);
	}

	private static BigInteger generateDHPubKey(BigInteger dhG, BigInteger dhP, PrivateKey rsaPrivKey) {
		BigInteger dhPrivKey = new BigInteger(rsaPrivKey.getEncoded());
		return dhG.modPow(dhPrivKey, dhP);
	}

	private static byte[] sign(PrivateKey key, byte[] bytes) {
		try {
			Signature signatureEngine = Signature.getInstance("SHA1withRSA");
			signatureEngine.initSign(key);

			signatureEngine.update(bytes);

			byte[] signature = signatureEngine.sign();
			return signature;
		} catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	public static boolean verify(PublicKey key, byte[] bytes, byte[] signature) {
		try {
			Signature signatureEngine = Signature.getInstance("SHA1withRSA");
			signatureEngine.initVerify(key);

			signatureEngine.update(bytes);
			return signatureEngine.verify(signature);
		} catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	private static class MacKeyEntry implements MacKey {

		private SecretKey key;

		private final Object mutxGen = new Object();
		private Mac macGen;
		
		private final Object mutxAuth = new Object();
		private Mac macAuth;

		public MacKeyEntry(SecretKey key) {
			this.key = key;
			macGen = createMac(key);
			macAuth = createMac(key);
		}

		private Mac createMac(SecretKey key) {
			try {
				Mac mac = Mac.getInstance(MAC_ALGORITHM);
				mac.init(key);
				return mac;
			} catch (NoSuchAlgorithmException | InvalidKeyException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
		}

		@Override
		public SecretKey getSecretKey() {
			return key;
		}

		@Override
		public byte[] generateMac(byte[] message) {
			synchronized (mutxGen) {
				return macGen.doFinal(message);
			}
		}

		@Override
		public synchronized boolean authenticate(byte[] message, byte[] mac) {
			byte[] expectedMac;
			synchronized (mutxAuth) {
				expectedMac = macAuth.doFinal(message);
			}
			return Arrays.areEqual(expectedMac, mac);
		}

	}

	private static class DHPubKeyCertEntry implements DHPubKeyCertificate {

		private BigInteger dhPubKey;

		private byte[] signatureBytes;

		public DHPubKeyCertEntry(BigInteger dhPubKey, byte[] signatureBytes) {
			this.dhPubKey = dhPubKey;
			this.signatureBytes = signatureBytes;
		}

		@Override
		public BigInteger getDHPubKey() {
			return dhPubKey;
		}

		@Override
		public byte[] getSignature() {
			return signatureBytes.clone();
		}

		@Override
		public byte[] getEncodedBytes() {
			byte[] dhPubKeyBytes = dhPubKey.toByteArray();
			byte[] dhPubKeyLengthBytes = BytesUtils.toBytes(dhPubKeyBytes.length);
			byte[] signatureLengthBytes = BytesUtils.toBytes(signatureBytes.length);
			return BytesUtils.concat(dhPubKeyLengthBytes, dhPubKeyBytes, signatureLengthBytes, signatureBytes);
		}

	}
}
