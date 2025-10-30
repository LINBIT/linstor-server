package com.linbit.linstor.systemstarter;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.core.SatelliteNetComInitializer;
import com.linbit.linstor.security.AccessContext;

public class NetComInitializer implements StartupInitializer
{
    private SatelliteNetComInitializer sncInitializer;
    private AccessContext initCtx;

    public NetComInitializer(
        SatelliteNetComInitializer sncInitializerRef,
        AccessContext initCtxRef
    )
    {
        sncInitializer = sncInitializerRef;
        initCtx = initCtxRef;
    }

    @Override
    public void initialize() throws SystemServiceStartException
    {
        if (!sncInitializer.initMainNetComService(initCtx))
        {
            throw new SystemServiceStartException("Initialisation of SatelliteNetComServices failed.", true);
        }
    }

    @Override
    public void shutdown(boolean jvmShutdownRef)
    {
        sncInitializer.shutdown(jvmShutdownRef);
    }

    @Override
    public void awaitShutdown(long timeout) throws InterruptedException
    {
        sncInitializer.awaitShutdown(timeout);
    }
}
