package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.interfaces.PropsDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsDatabaseDriver.PropsDbEntry;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.Map;

@Singleton
public class SatellitePropDriver
    extends AbsSatelliteDbDriver<PropsDbEntry>
    implements PropsDatabaseDriver
{
    private final SingleColumnDatabaseDriver<PropsDbEntry, String> valueDriver;

    @Inject
    public SatellitePropDriver()
    {
        valueDriver = getNoopColumnDriver();
    }

    @Override
    public Map<String, String> loadCachedInstance(@Nullable String propsInstanceRef)
    {
        return Collections.emptyMap();
    }

    @Override
    public SingleColumnDatabaseDriver<PropsDbEntry, String> getValueDriver()
    {
        return valueDriver;
    }
}
