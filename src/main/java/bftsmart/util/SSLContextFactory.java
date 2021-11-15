package bftsmart.util;

import utils.StringUtils;
import utils.net.SSLSecurity;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

/**
 * @description: SSL Context构造
 * @author: imuge
 * @date: 2021/11/15
 **/
public class SSLContextFactory {

    public static SSLContext getSSLContext(boolean isClient, SSLSecurity sslSecurity) throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        TrustManagerFactory tmf;
        KeyStore trustStore;
        TrustManager[] tms;
        switch (sslSecurity.getSslMode(isClient)) {
            case OFF:
                context.init(null, new TrustManager[]{new X509TrustManager() {

                    @Override
                    public void checkClientTrusted(X509Certificate[] ax509certificate, String s) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] ax509certificate, String s) {
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                }}, null);
                break;
            case ONE_WAY:
                tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustStore = KeyStore.getInstance("JKS");
                trustStore.load(new FileInputStream(sslSecurity.getTrustStore()), sslSecurity.getTrustStorePassword().toCharArray());
                tmf.init(trustStore);
                tms = tmf.getTrustManagers();
                context.init(null, tms, new SecureRandom());
                break;
            case TWO_WAY:
                KeyManager[] kms = null;
                if (!StringUtils.isEmpty(sslSecurity.getKeyStore())) {
                    KeyStore clientStore = KeyStore.getInstance(sslSecurity.getTrustStoreType());
                    clientStore.load(new FileInputStream(sslSecurity.getKeyStore()), sslSecurity.getKeyStorePassword().toCharArray());
                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(clientStore, sslSecurity.getKeyStorePassword().toCharArray());
                    kms = kmf.getKeyManagers();
                }
                tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustStore = KeyStore.getInstance(sslSecurity.getTrustStoreType());
                trustStore.load(new FileInputStream(sslSecurity.getTrustStore()), sslSecurity.getTrustStorePassword().toCharArray());
                tmf.init(trustStore);
                tms = tmf.getTrustManagers();
                context.init(kms, tms, new SecureRandom());
                break;
        }

        return context;
    }

}
