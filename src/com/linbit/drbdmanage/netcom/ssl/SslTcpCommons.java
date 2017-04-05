package com.linbit.drbdmanage.netcom.ssl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

// TODO: Maybe this whole class should be merged into SslTcpConnectorPeer
public class SslTcpCommons
{
    public static final int DEFAULT_MAX_DATA_SIZE = 0x1000000; // 16 MiB

    public static KeyManager[] createKeyManagers(
        final String file,
        final char[] keyStorePasswd,
        final char[] keyPasswd
    )
        throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
        UnrecoverableKeyException
    {
        return createKeyManagers(
            file, keyStorePasswd, keyPasswd,
            SslTcpConstants.KEY_STORE_DEFAULT_TYPE, KeyManagerFactory.getDefaultAlgorithm()
        );
    }

    public static KeyManager[] createKeyManagers(
        final String file,
        final char[] keyStorePasswd,
        final char[] keyPasswd,
        final String keyStoreType,
        final String keyManagerFactoryAlgorithm
    )
        throws KeyStoreException, IOException, NoSuchAlgorithmException,
        CertificateException, UnrecoverableKeyException
    {
        final KeyStore keyStore = loadStore(file, keyStoreType, keyStorePasswd);
        KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(keyManagerFactoryAlgorithm);
        keyMgrFactory.init(keyStore, keyPasswd);
        return keyMgrFactory.getKeyManagers();
    }

    public static TrustManager[] createTrustManagers(
        final String file,
        final char[] keyStorePasswd
    )
        throws KeyStoreException, FileNotFoundException, NoSuchAlgorithmException, IOException, CertificateException
    {
        return createTrustManagers(
            file, keyStorePasswd,
            SslTcpConstants.TRUST_STORE_DEFAULT_TYPE, TrustManagerFactory.getDefaultAlgorithm()
        );
    }

    public static TrustManager[] createTrustManagers(
        final String file,
        final char[] trustStorePasswd,
        final String trustStoreType,
        final String trustManagerFactoryAlgorithm
    )
        throws KeyStoreException, FileNotFoundException, IOException, NoSuchAlgorithmException, CertificateException
    {
        KeyStore trustStore = loadStore(file, trustStoreType, trustStorePasswd);
        TrustManagerFactory trustMgrFactory = TrustManagerFactory.getInstance(trustManagerFactoryAlgorithm);
        trustMgrFactory.init(trustStore);
        return trustMgrFactory.getTrustManagers();
    }

    private static KeyStore loadStore(
        final String file,
        final String storeType,
        final char[] storePasswd
    )
        throws KeyStoreException, IOException, FileNotFoundException, NoSuchAlgorithmException, CertificateException
    {
        KeyStore store = KeyStore.getInstance(storeType);
        try (InputStream keyStoreIS = new FileInputStream(file))
        {
            store.load(keyStoreIS, storePasswd);
        }
        return store;
    }
}
