package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.VolumeConnection;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDatabaseDriver;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;

public class SatelliteVolConDfnDriver implements VolumeConnectionDatabaseDriver
{
    private final AccessContext dbCtx;

    @Inject
    public SatelliteVolConDfnDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @Override
    public void create(VolumeConnection vlmConn)
    {
        // no-op
    }

    @Override
    public void delete(VolumeConnection vlmConn)
    {
        // no-op
    }
}
