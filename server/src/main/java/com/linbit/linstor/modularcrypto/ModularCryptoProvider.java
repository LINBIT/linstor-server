package com.linbit.linstor.modularcrypto;

import com.linbit.crypto.SecretGenerator;
import com.linbit.crypto.ByteArrayCipher;
import com.linbit.crypto.KeyDerivation;
import com.linbit.crypto.LengthPadding;
import com.linbit.linstor.LinStorException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

public interface ModularCryptoProvider
{
    boolean isFips140Enabled();

    String getModuleIdentifier();

    SecureRandom createSecureRandom();

    ByteArrayCipher createCipherWithKey(byte[] key)
        throws LinStorException;

    ByteArrayCipher createCipherWithPassword(byte[] password, byte[] salt)
        throws LinStorException;

    KeyDerivation createKeyDerivation()
        throws LinStorException;

    SecretGenerator createSecretGenerator();

    LengthPadding createLengthPadding();

    SSLContext createSslContext(String sslProtocol)
        throws NoSuchAlgorithmException;

    void initializeSslContext(
        SSLContext sslCtx,
        String keyStoreFile,
        char[] keyStorePassword,
        char[] keyPassword,
        String trustStoreFile,
        char[] trustStorePassword
    )
        throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
            UnrecoverableKeyException, KeyManagementException;

    KeyManager[] createKeyManagers(
        final String file,
        final char[] keyStorePassword,
        final char[] keyPassword
    )
        throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
            UnrecoverableKeyException;

    TrustManager[] createTrustManagers(
        final String file,
        final char[] trustStorePassword
    )
        throws KeyStoreException, FileNotFoundException, NoSuchAlgorithmException, IOException, CertificateException;
}
