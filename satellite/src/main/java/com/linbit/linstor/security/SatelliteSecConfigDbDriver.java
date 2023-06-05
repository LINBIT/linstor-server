package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.SatelliteSingleColDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecConfigDbDriver implements SecConfigDatabaseDriver
{
    private final SingleColumnDatabaseDriver<SecConfigDbEntry, String> noopValueDriver;

    @Inject
    public SatelliteSecConfigDbDriver()
    {
        noopValueDriver = new SatelliteSingleColDriver<>();
    }

    @Override
    public void create(SecConfigDbEntry dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public void delete(SecConfigDbEntry dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public SingleColumnDatabaseDriver<SecConfigDbEntry, String> getValueDriver()
    {
        return noopValueDriver;
    }
}
