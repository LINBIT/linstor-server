package com.linbit.linstor.dbcp.migration;

import com.linbit.ImplementationError;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;

public abstract class AbsMigration
{
    private static ModularCryptoProvider cryptProvider;

    public static void setModularCryptoProvider(ModularCryptoProvider cryptProviderRef)
    {
        if (cryptProvider != null && cryptProviderRef != cryptProvider)
        {
            throw new ImplementationError("ModularCryptoProvider already set");
        }
        cryptProvider = cryptProviderRef;
    }

    protected ModularCryptoProvider getCryptoProvider()
    {
        return cryptProvider;
    }
}
