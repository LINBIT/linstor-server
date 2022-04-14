package com.linbit.linstor.modularcrypto;

import com.linbit.crypto.SecretGenerator;
import com.linbit.crypto.ByteArrayCipher;
import com.linbit.crypto.KeyDerivation;
import com.linbit.crypto.LengthPadding;
import com.linbit.linstor.LinStorException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.inject.Singleton;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.inject.Inject;

@Singleton
public class JclCryptoProvider implements ModularCryptoProvider
{
    private final SecretGenerator   secretGen;
    private final LengthPadding     cryptoLenPad;

    @Inject
    public JclCryptoProvider()
    {
        secretGen = new SecretGeneratorImpl(new SecureRandom());
        cryptoLenPad = new LengthPaddingImpl(new SecureRandom());
    }

    @Override
    public boolean isFips140Enabled()
    {
        return false;
    }

    @Override
    public String getModuleIdentifier()
    {
        return "default cryptography module";
    }

    @Override
    public SecureRandom createSecureRandom()
    {
        return new SecureRandom();
    }

    @Override
    public ByteArrayCipher createCipherWithKey(byte[] key)
        throws LinStorException
    {
        return JclSymmetricKeyCipher.getInstanceWithKey(key);
    }

    @Override
    public ByteArrayCipher createCipherWithPassword(byte[] password, byte[] salt)
        throws LinStorException
    {
        return JclSymmetricKeyCipher.getInstanceWithPassword(password, salt);
    }

    @Override
    public KeyDerivation createKeyDerivation()
        throws LinStorException
    {
        return new JclKeyDerivationImpl();
    }

    @Override
    public SecretGenerator createSecretGenerator()
    {
        return secretGen;
    }

    @Override
    public LengthPadding createLengthPadding()
    {
        return cryptoLenPad;
    }

    @Override
    public SSLContext createSslContext(String sslProtocol)
        throws NoSuchAlgorithmException
    {
        return SSLContext.getInstance(sslProtocol);
    }

    @Override
    public void initializeSslContext(
        SSLContext sslCtx,
        String keyStoreFile,
        char[] keyStorePassword,
        char[] keyPassword,
        String trustStoreFile,
        char[] trustStorePassword
    )
        throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
            UnrecoverableKeyException, KeyManagementException
    {
        sslCtx.init(
            createKeyManagers(keyStoreFile, keyStorePassword, keyPassword),
            createTrustManagers(trustStoreFile, trustStorePassword),
            new SecureRandom()
        );
    }

    @Override
    public KeyManager[] createKeyManagers(
        String file,
        char[] keyStorePassword,
        char[] keyPassword
    )
        throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
            UnrecoverableKeyException
    {
        final KeyStore keyStore = loadStore(file, keyStorePassword);
        KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(
            KeyManagerFactory.getDefaultAlgorithm()
        );
        keyMgrFactory.init(keyStore, keyPassword);
        return keyMgrFactory.getKeyManagers();
    }

    @Override
    public TrustManager[] createTrustManagers(
        String file,
        char[] trustStorePassword
    )
        throws KeyStoreException, FileNotFoundException, NoSuchAlgorithmException, IOException,
            CertificateException
    {
        KeyStore trustStore;
        try
        {
            trustStore = loadStore(file, trustStorePassword);
        }
        catch (FileNotFoundException fileNotFoundExc)
        {
            trustStore = null;
        }
        TrustManagerFactory trustMgrFactory = TrustManagerFactory.getInstance(
            TrustManagerFactory.getDefaultAlgorithm()
        );
        trustMgrFactory.init(trustStore);
        return trustMgrFactory.getTrustManagers();
    }

    private static KeyStore loadStore(
        final String file,
        final char[] storePasswd
    )
        throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException
    {
        KeyStore store = KeyStore.getInstance("JKS");
        try (InputStream keyStoreIS = new FileInputStream(file))
        {
            store.load(keyStoreIS, storePasswd);
        }
        return store;
    }
}
