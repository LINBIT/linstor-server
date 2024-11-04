package com.linbit.linstor.modularcrypto;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Dynamic cryptography provider loader for unit tests
 */
public class CryptoProviderLoader
{
    private static final String[] PROVIDER_CLASSES =
    {
        "com.linbit.linstor.modularcrypto.FipsCryptoProvider",
        "com.linbit.linstor.modularcrypto.JclCryptoProvider"
    };

    public static ModularCryptoProvider loadCryptoProvider()
        throws ClassNotFoundException
    {
        ModularCryptoProvider cryptoProvider = null;

        for (int idx = 0; cryptoProvider == null && idx < PROVIDER_CLASSES.length; ++idx)
        {
            final String providerName = PROVIDER_CLASSES[idx];
            try
            {
                final Class<?> providerClass = Class.forName(providerName);
                Constructor<?> providerConstr = providerClass.getDeclaredConstructor();
                cryptoProvider = (ModularCryptoProvider) providerConstr.newInstance();
            }
            catch (IllegalAccessException | InstantiationException | InvocationTargetException |
                   NoSuchMethodException | ClassCastException exc)
            {
                final String errorMsg = exc.getMessage();
                System.err.println(
                    CryptoProviderLoader.class.getSimpleName() + ": Cannot load " +
                    ModularCryptoProvider.class.getSimpleName() + " implementation " + providerName +
                    ": Initialization error" +
                    (errorMsg == null ? "" : ("\n" + errorMsg))
                );
                throw new RuntimeException(
                    CryptoProviderLoader.class.getSimpleName() + ": Cryptography provider class loading failed"
                );
            }
            catch (ClassNotFoundException ignored)
            {
            }
        }

        if (cryptoProvider == null)
        {
            throw new ClassNotFoundException(
                CryptoProviderLoader.class.getSimpleName() + ": Could not find any " +
                ModularCryptoProvider.class.getSimpleName() + " implementations to load"
            );
        }

        return cryptoProvider;
    }
}
