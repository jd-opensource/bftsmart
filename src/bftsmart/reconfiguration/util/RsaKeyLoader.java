package bftsmart.reconfiguration.util;

import java.security.PrivateKey;
import java.security.PublicKey;

public interface RsaKeyLoader {

	/**
	 * Loads the public key of some processes from configuration files
	 *
	 * @return the PublicKey loaded from config/keys/publickey<id>
	 * @throws Exception problems reading or parsing the key
	 */
	PublicKey loadPublicKey(int id) throws Exception;

//	PublicKey loadPublicKey() throws Exception;

	/**
	 * Loads the private key of this process
	 *
	 * @return the PrivateKey loaded from config/keys/publickey<conf.getProcessId()>
	 * @throws Exception problems reading or parsing the key
	 */
	PrivateKey loadPrivateKey(int id) throws Exception;

}