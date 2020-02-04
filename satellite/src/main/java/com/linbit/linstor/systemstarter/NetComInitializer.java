package com.linbit.linstor.systemstarter;

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
    public void initialize() throws NetComServiceException
    {
        if (!sncInitializer.initMainNetComService(initCtx))
        {
            throw new NetComServiceException("Initialisation of SatelliteNetComServices failed.");
        }
    }
}
