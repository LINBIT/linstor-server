package com.linbit.linstor.modularcrypto;

import com.google.inject.AbstractModule;

public class JclCryptoModule extends AbstractModule
{
    private static final ModularCryptoProvider CRYPTO_PROVIDER_INSTANCE = new JclCryptoProvider();

    @Override
    protected void configure()
    {
        bind(ModularCryptoProvider.class).toInstance(CRYPTO_PROVIDER_INSTANCE);
    }
}
