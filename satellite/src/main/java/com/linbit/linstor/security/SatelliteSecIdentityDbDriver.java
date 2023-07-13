package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.AbsSatelliteDbDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityDatabaseDriver.SecIdentityDbObj;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecIdentityDbDriver
    extends AbsSatelliteDbDriver<SecIdentityDbObj>
    implements SecIdentityDatabaseDriver
{
    private final SingleColumnDatabaseDriver<SecIdentityDbObj, byte[]> noopPassHashDriver;
    private final SingleColumnDatabaseDriver<SecIdentityDbObj, byte[]> noopPassSaltDriver;
    private final SingleColumnDatabaseDriver<SecIdentityDbObj, Boolean> noopIdEnabledDriver;
    private final SingleColumnDatabaseDriver<SecIdentityDbObj, Boolean> noopIdLockedDriver;

    @Inject
    public SatelliteSecIdentityDbDriver()
    {
        noopPassHashDriver = getNoopColumnDriver();
        noopPassSaltDriver = getNoopColumnDriver();
        noopIdEnabledDriver = getNoopColumnDriver();
        noopIdLockedDriver = getNoopColumnDriver();
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
