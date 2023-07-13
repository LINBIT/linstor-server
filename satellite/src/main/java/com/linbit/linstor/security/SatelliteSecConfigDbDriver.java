package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.AbsSatelliteDbDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver.SecConfigDbEntry;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecConfigDbDriver
    extends AbsSatelliteDbDriver<SecConfigDbEntry>
    implements SecConfigDatabaseDriver
{
    private final SingleColumnDatabaseDriver<SecConfigDbEntry, String> noopValueDriver;

    @Inject
    public SatelliteSecConfigDbDriver()
    {
        noopValueDriver = getNoopColumnDriver();
    }

    @Override
    public SingleColumnDatabaseDriver<SecConfigDbEntry, String> getValueDriver()
    {
        return noopValueDriver;
    }
}
