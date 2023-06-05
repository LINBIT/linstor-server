package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecConfigDbDriver implements SecConfigDatabaseDriver
{
    @Inject
    public SatelliteSecConfigDbDriver()
    {
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
}
