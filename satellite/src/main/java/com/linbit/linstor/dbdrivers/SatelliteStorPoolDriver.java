package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDatabaseDriver;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;

public class SatelliteStorPoolDriver implements StorPoolDatabaseDriver
{
    private final AccessContext dbCtx;

    @Inject
    public SatelliteStorPoolDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @Override
    public void create(StorPool storPool)
    {
        // no-op
    }

    @Override
    public void delete(StorPool data)
    {
        // no-op
    }
}
