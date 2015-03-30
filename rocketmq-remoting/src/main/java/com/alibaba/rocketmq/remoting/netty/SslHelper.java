package com.alibaba.rocketmq.remoting.netty;

import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.SSLContextCreationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.Arrays;

public class SslHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);

    private static final String DEFAULT_SERVER_PASSWORD = "VVYZZ9NLVdy849XIy/tM3Q==";

    private static final String DEFAULT_CLIENT_PASSWORD = "VVYZZ9NLVdy849XIy/tM3Q==";

    private static final String INITIAL_VECTOR = "0102030405060708";

    private static final String CIPHER = "AES/CBC/PKCS5Padding";

    private static final String SECRET = "R0ck1tMQ@NDPMediaP1ssw0rd";

    private static final String DEFAULT_KEY_STORE_PATH = "/dianyi/conf/RocketMQ/SSL/";

    private static final String SERVER_KEY_STORE_FILE_NAME = "server.ks";

    private static final String CLIENT_KEY_STORE_FILE_NAME = "client.ks";

    public static SSLContext getSSLContext(SslRole role) throws SSLContextCreationException {
        try {
            SSLContext sslContext = SSLContext.getInstance("SSL");
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore keyStore = KeyStore.getInstance("JKS");
            KeyStore trustKeyStore = KeyStore.getInstance("JKS");

            String keyStoreLocation = System.getProperty("RocketMQ_KeyStore_Dir", DEFAULT_KEY_STORE_PATH);
            File keyStoreDir = new File(keyStoreLocation);
            boolean customKeyStoreDirExists = keyStoreDir.exists();
            boolean customServerKeyStoreExists = false;
            boolean customClientKeyStoreExists = false;

            File customServerKeyStore = null;
            File customClientKeyStore = null;

            if (customKeyStoreDirExists) {
                customServerKeyStore = new File(keyStoreDir, SERVER_KEY_STORE_FILE_NAME);
                customServerKeyStoreExists = customServerKeyStore.exists();

                customClientKeyStore = new File(keyStoreDir, CLIENT_KEY_STORE_FILE_NAME);
                customClientKeyStoreExists = customClientKeyStore.exists();
            }

            ClassLoader classLoader = SslHelper.class.getClassLoader();
            switch (role) {
                case SERVER:
                    String cipherText = System.getProperty("RocketMQServerPassword", DEFAULT_SERVER_PASSWORD);
                    char[] clearText = decrypt(SECRET, cipherText);
                    InputStream serverKeyStoreInputStream = null;
                    ByteArrayOutputStream byteArrayOutputStream = null;
                    try {
                        if (customServerKeyStoreExists) {
                            if (!customServerKeyStore.canRead()) {
                                throw new IOException("No permission to read:" +
                                        customServerKeyStore.getCanonicalPath());
                            }
                            serverKeyStoreInputStream = new FileInputStream(customServerKeyStore);
                            LOGGER.info("Production environment, using server key store:"
                                    + customServerKeyStore.getCanonicalPath());
                        } else {
                            serverKeyStoreInputStream = classLoader.getResourceAsStream(SERVER_KEY_STORE_FILE_NAME);
                            LOGGER.info("Development environment, using built-in server key store.");
                        }

                        byteArrayOutputStream = new ByteArrayOutputStream();
                        byte[] buffer = new byte[1024];
                        int len = 0;
                        while ((len = serverKeyStoreInputStream.read(buffer)) != -1) {
                            byteArrayOutputStream.write(buffer, 0, len);
                        }
                    } finally {
                        if (null != serverKeyStoreInputStream) {
                            serverKeyStoreInputStream.close();
                        }
                    }
                    keyStore.load(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()), clearText);
                    trustKeyStore.load(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()), clearText);
                    keyManagerFactory.init(keyStore, clearText);
                    trustManagerFactory.init(trustKeyStore);
                    break;

                case CLIENT:
                    cipherText = System.getProperty("RocketMQClientPassword", DEFAULT_CLIENT_PASSWORD);
                    clearText = decrypt(SECRET, cipherText);

                    InputStream clientKeyStoreInputStream = null;
                    byteArrayOutputStream = null;
                    try {
                        if (customClientKeyStoreExists) {
                            if (!customClientKeyStore.canRead()) {
                                throw new IOException("No permission to read: "
                                        + customClientKeyStore.getCanonicalPath());
                            }
                            clientKeyStoreInputStream = new FileInputStream(customClientKeyStore);
                            LOGGER.info("Production environment, using client key store: "
                                    + customClientKeyStore.getCanonicalPath());
                        } else {
                            clientKeyStoreInputStream = classLoader.getResourceAsStream(CLIENT_KEY_STORE_FILE_NAME);

                            if (null == clientKeyStoreInputStream) {
                                LOGGER.error("Unexpected null!!!!");
                            }

                            LOGGER.info("Development environment, using built-in client key store.");
                        }

                        byteArrayOutputStream = new ByteArrayOutputStream();
                        byte[] buffer = new byte[1024];
                        int len = 0;
                        while ((len = clientKeyStoreInputStream.read(buffer)) != -1) {
                            byteArrayOutputStream.write(buffer, 0, len);
                        }
                    } finally {
                        if (null != clientKeyStoreInputStream) {
                            clientKeyStoreInputStream.close();
                        }
                    }

                    keyStore.load(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()), clearText);
                    trustKeyStore.load(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()), clearText);
                    keyManagerFactory.init(keyStore, clearText);
                    trustManagerFactory.init(trustKeyStore);
                    break;
            }
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
            return sslContext;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            throw new SSLContextCreationException("Error while creating SSLContext", e);
        } catch (KeyStoreException e) {
            e.printStackTrace();
            throw new SSLContextCreationException("Error while creating SSLContext", e);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SSLContextCreationException("Error while creating SSLContext", e);
        } catch (CertificateException e) {
            e.printStackTrace();
            throw new SSLContextCreationException("Error while creating SSLContext", e);
        } catch (KeyManagementException e) {
            e.printStackTrace();
            throw new SSLContextCreationException("Error while creating SSLContext", e);
        } catch (UnrecoverableKeyException e) {
            e.printStackTrace();
            throw new SSLContextCreationException("Error while creating SSLContext", e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SSLContextCreationException("Error while creating SSLContext", e);
        }
    }


    public static SSLEngine getSSLEngine(SSLContext context, SslRole role) {
        SSLEngine engine = context.createSSLEngine();
        switch (role) {
            case SERVER:
                engine.setUseClientMode(false);
                break;
            case CLIENT:
                engine.setUseClientMode(true);
                break;
        }
        return engine;
    }

    public static String encrypt(String secret, String clearText) throws Exception {
        SecretKeySpec secretKeySpec = getKey(secret);
        Cipher cipher = Cipher.getInstance(CIPHER);
        IvParameterSpec iv = new IvParameterSpec(INITIAL_VECTOR.getBytes());
        cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, iv);
        byte[] encrypted = cipher.doFinal(clearText.getBytes());
        return new BASE64Encoder().encode(encrypted);
    }

    public static char[] decrypt(String secret, String cipherText) throws Exception {
        SecretKeySpec secretKeySpec = getKey(secret);
        Cipher cipher = Cipher.getInstance(CIPHER);
        IvParameterSpec iv = new IvParameterSpec(INITIAL_VECTOR.getBytes());
        cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, iv);
        byte[] encodeCipherText = new BASE64Decoder().decodeBuffer(cipherText);
        return new String(cipher.doFinal(encodeCipherText)).toCharArray();
    }

    private static SecretKeySpec getKey(String secret) throws Exception {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] key = sha1.digest(secret.getBytes());
        return new SecretKeySpec(Arrays.copyOf(key, 16), "AES");
    }
}

enum SslRole {
    CLIENT, SERVER
}
