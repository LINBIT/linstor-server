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
    private final SingleColumnDatabaseDriver<SecIdentityDbObj, byte[]> noopPassHashDriver;
    private final SingleColumnDatabaseDriver<SecIdentityDbObj, byte[]> noopPassSaltDriver;
    private final SingleColumnDatabaseDriver<SecIdentityDbObj, Boolean> noopIdEnabledDriver;
    private final SingleColumnDatabaseDriver<SecIdentityDbObj, Boolean> noopIdLockedDriver;

    @Inject
    public SatelliteSecIdentityDbDriver()
    {
        noopPassHashDriver = new SatelliteSingleColDriver<>();
        noopPassSaltDriver = new SatelliteSingleColDriver<>();
        noopIdEnabledDriver = new SatelliteSingleColDriver<>();
        noopIdLockedDriver = new SatelliteSingleColDriver<>();
    }

    @Override
    public void create(SecIdentityDbObj dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public void delete(SecIdentityDbObj dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public SingleColumnDatabaseDriver<SecIdentityDbObj, byte[]> getPassHashDriver()
    {
        return noopPassHashDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<SecIdentityDbObj, byte[]> getPassSaltDriver()
    {
        return noopPassSaltDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<SecIdentityDbObj, Boolean> getIdEnabledDriver()
    {
        return noopIdEnabledDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<SecIdentityDbObj, Boolean> getIdLockedDriver()
    {
        return noopIdLockedDriver;
    }
}
