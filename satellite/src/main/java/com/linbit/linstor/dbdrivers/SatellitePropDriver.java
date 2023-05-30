package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.PropsDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;

import java.util.Collections;
import java.util.Map;

public class SatellitePropDriver implements PropsDatabaseDriver
{
    private final SingleColumnDatabaseDriver<PropsDbEntry, String> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatellitePropDriver()
    {
        // no-op
    }

    @Override
    public void create(PropsDbEntry dataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(PropsDbEntry dataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public Map<String, String> loadCachedInstance(String propsInstanceRef)
    {
        return Collections.emptyMap();
    }

    @Override
    public SingleColumnDatabaseDriver<PropsDbEntry, String> getValueDriver()
    {
        return singleColDriver;
    }
}
