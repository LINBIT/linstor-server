package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.SatelliteSingleColDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecIdentityDbDriver implements SecIdentityDatabaseDriver
{
    private final SingleColumnDatabaseDriver<Identity, byte[]> noopPassHashDriver = new SatelliteSingleColDriver<>();
    private final SingleColumnDatabaseDriver<Identity, byte[]> noopPassSaltDriver = new SatelliteSingleColDriver<>();
    private final SingleColumnDatabaseDriver<Identity, Boolean> noopIdEnabledDriver = new SatelliteSingleColDriver<>();
    private final SingleColumnDatabaseDriver<Identity, Boolean> noopIdLockedDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteSecIdentityDbDriver()
    {
    }

    @Override
    public void create(Identity dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public void delete(Identity dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public SingleColumnDatabaseDriver<Identity, byte[]> getPassHashDriver()
    {
        return noopPassHashDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Identity, byte[]> getPassSaltDriver()
    {
        return noopPassSaltDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Identity, Boolean> getIdEnabledDriver()
    {
        return noopIdEnabledDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Identity, Boolean> getIdLockedDriver()
    {
        return noopIdLockedDriver;
    }
}
