package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.CryptSetupDatabaseDriver;
import com.linbit.linstor.storage.interfaces.layers.cryptsetup.CryptSetupVlmObject;

import javax.inject.Inject;

public class SatelliteCryptSetupDriver implements CryptSetupDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteCryptSetupDriver()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<CryptSetupVlmObject, char[]> getPasswordDriver()
    {
        return (SingleColumnDatabaseDriver<CryptSetupVlmObject, char[]>) singleColDriver;
    }

}
