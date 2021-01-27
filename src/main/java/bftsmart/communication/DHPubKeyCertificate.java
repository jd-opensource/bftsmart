package bftsmart.communication;

import java.math.BigInteger;

/**
 * DH 公钥凭证；<p>
 * 
 * @author huanghaiquan
 *
 */
public interface DHPubKeyCertificate {
	
	BigInteger getDHPubKey();
	
	byte[] getSignature();
	
	byte[] getEncodedBytes();
}
