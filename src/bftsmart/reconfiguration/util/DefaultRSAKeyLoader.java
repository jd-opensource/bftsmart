/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.reconfiguration.util;

import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import org.apache.commons.codec.binary.Base64;

/**
 * Used to load JCA public and private keys from conf/keys/publickey<id> and
 * conf/keys/privatekey<id>
 */
public class DefaultRSAKeyLoader implements RsaKeyLoader {

	private static String DEFAULT_UKEY = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCwuoTWbSFDnVjohwdZftoAwv3oCxUPnUiiNNH9\n"
			+ "\npXryEW8kSFRGVJ7zJCwxJnt3YZGnpPGxnC3hAI4XkG26hO7+TxkgaYmv5GbamL946uZISxv0aNX3\n"
			+ "\nYbaOf//MC6F8tShFfCnpWlj68FYulM5dC2OOOHaUJfofQhmXfsaEWU251wIDAQAB";

	private static String DEFAULT_PKEY = "MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBALC6hNZtIUOdWOiHB1l+2gDC/egL\n"
			+ "\nFQ+dSKI00f2levIRbyRIVEZUnvMkLDEme3dhkaek8bGcLeEAjheQbbqE7v5PGSBpia/kZtqYv3jq\n"
			+ "\n5khLG/Ro1fdhto5//8wLoXy1KEV8KelaWPrwVi6Uzl0LY444dpQl+h9CGZd+xoRZTbnXAgMBAAEC\n"
			+ "\ngYAJaUVdrd4RnbV4XIh1qZ2uYLPowX5ToIqXqLxuB3vunCMRCZEDVcpJJGn+DBCTIO0CwnPkg26m\n"
			+ "\nBsOKWbSeNCoN5gOi5yd6Poe0D40ZmvHP1hMCQ9LYhwjLB3Aa+Cl5gYL074Qe/eJFqJaYjZApkeJU\n"
			+ "\nAy1HkXhM5OBW9grrXxg6YQJBAPTIni5fG5f2SYutkR2pUydZP4haKVabRkZr8nSHuClGDE2HzbNQ\n"
			+ "\njb17z5rRVxJCKMLb2HiPg7ZsUgGK/J1ri78CQQC405h45rL1mCIyZQCXcK/nQZTVi8UaaelKN/ub\n"
			+ "\nLQKtTGenJao/zoL+m39i+gGRkHWiG6HNaGFdOkRJmeeH+rfpAkEAn0fwDjKbDP4ZC0fM1uU4k7Ey\n"
			+ "\nczJgFdgCGY7ifMtXnZvUI5sL0fPH15W6GH7BzsK4LVvK92BDj6/aiOB80p6JlwJASjL4NSE4mwv2\n"
			+ "\nPpD5ydI9a/OSEqDIAjCerWMIKWXKe1P/EMU4MeFwCVLXsx523r9F2kyJinLrE4g+veWBY7+tcQJB\n"
			+ "\nAKCTm3tbbwLJnnPN46mAgrYb5+LFOmHyNtEDgjxEVrzpQaCChZici2YGY1jTBjb/De4jii8RXllA\n" + "\ntUhBEsqyXDA=";

	private PublicKey pubKey;
	private PrivateKey priKey;

	public DefaultRSAKeyLoader() {
		try {
			this.pubKey = RSAKeyUtils.getPublicKeyFromString(DefaultRSAKeyLoader.DEFAULT_UKEY);
			this.priKey = RSAKeyUtils.getPrivateKeyFromString(DefaultRSAKeyLoader.DEFAULT_PKEY);
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see bftsmart.reconfiguration.util.KeyLoader#loadPublicKey(int)
	 */
	@Override
	public PublicKey loadPublicKey(int id) throws Exception {
		return pubKey;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see bftsmart.reconfiguration.util.KeyLoader#loadPrivateKey()
	 */
	@Override
	public PrivateKey loadPrivateKey(int id) throws Exception {
		return priKey;
	}

}
