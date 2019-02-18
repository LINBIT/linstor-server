package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.CryptSetupLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.cryptsetup.CryptSetupRscData;
import com.linbit.linstor.storage.data.adapter.cryptsetup.CryptSetupVlmData;
import com.linbit.linstor.storage.interfaces.layers.cryptsetup.CryptSetupVlmObject;

import javax.inject.Inject;

import java.sql.SQLException;

public class SatelliteCryptSetupDriver implements CryptSetupLayerDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();
    private final ResourceLayerIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteResourceLayerIdDriver();

    @Inject
    public SatelliteCryptSetupDriver()
    {
    }


    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public void persist(CryptSetupRscData cryptRscDataRef) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(CryptSetupRscData cryptRscDataRef) throws SQLException
    {
        // no-op
    }

    @Override
    public void persist(CryptSetupVlmData cryptVlmDataRef) throws SQLException
    {
        // no-op
    }

    @Override
    public void delete(CryptSetupVlmData cryptVlmDataRef) throws SQLException
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<CryptSetupVlmData, byte[]> getVlmEncryptedPasswordDriver()
    {
        return (SingleColumnDatabaseDriver<CryptSetupVlmData, byte[]>) noopSingleColDriver;
    }
}
