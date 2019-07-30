package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.StorPoolData;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import javax.inject.Inject;

public class SatelliteStorPoolDriver implements StorPoolDataDatabaseDriver
{
    private final AccessContext dbCtx;

    @Inject
    public SatelliteStorPoolDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @Override
    public void create(StorPoolData storPoolData)
    {
        // no-op
    }

    @Override
    public void delete(StorPoolData data)
    {
        // no-op
    }

    @Override
    public void ensureEntryExists(StorPoolData data)
    {
        // no-op
    }
}
