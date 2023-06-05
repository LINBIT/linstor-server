package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.SatelliteSingleColDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecTypeDbDriver implements SecTypeDatabaseDriver
{
    private SingleColumnDatabaseDriver<SecurityType, Boolean> typeEnabledDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteSecTypeDbDriver()
    {
    }

    @Override
    public void create(SecurityType dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public void delete(SecurityType dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public SingleColumnDatabaseDriver<SecurityType, Boolean> getTypeEnabledDriver()
    {
        return typeEnabledDriver;
    }
}
