package bftsmart.consensus.app;

import org.bouncycastle.crypto.digests.SHA256Digest;

public class SHA256Utils {

    // The length of SHA256 output is 32 bytes
    private static final int SHA256DIGEST_LENGTH = 256 / 8;

    public byte[] hash(byte[] data){

        byte[] result = new byte[SHA256DIGEST_LENGTH];
        SHA256Digest sha256Digest = new SHA256Digest();

        sha256Digest.update(data,0,data.length);
        sha256Digest.doFinal(result,0);
        return result;
    }
}
