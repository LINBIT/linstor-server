package com.linbit.linstor.netcom.ssl;

import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

public class SslTcpConstants
{
    public static final String KEY_STORE_DEFAULT_TYPE = KeyStore.getDefaultType();
    public static final String KEY_MANAGER_FACTORY_DEFAULT_TYPE = KeyManagerFactory.getDefaultAlgorithm();
    public static final String TRUST_STORE_DEFAULT_TYPE = KeyStore.getDefaultType();
    public static final String TRUST_MANAGER_FACTORY_DEFAULT_TYPE = TrustManagerFactory.getDefaultAlgorithm();
    public static final String SSL_CONTEXT_DEFAULT_TYPE = "TLSv1.2";
    public static final String SSL_CONTEXT_DEFAULT_HOST = "localhost";
    public static final int SSL_CONTEXT_DEFAULT_PORT = 9922;

    public static final int OUT_BUFFER_ADDITIONAL_SIZE = 50;

    private SslTcpConstants()
    {
    }

}
