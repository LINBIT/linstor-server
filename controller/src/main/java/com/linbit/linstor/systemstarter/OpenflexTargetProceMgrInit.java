package com.linbit.linstor.systemstarter;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.core.SpecialSatelliteProcessManager;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;

public class OpenflexTargetProceMgrInit implements StartupInitializer
{
    private final SpecialSatelliteProcessManager openFlexTargetProcessManager;

    @Inject
    public OpenflexTargetProceMgrInit(SpecialSatelliteProcessManager openFlexTargetProcessManagerRef)
    {
        openFlexTargetProcessManager = openFlexTargetProcessManagerRef;
    }

    @Override
    public void initialize() throws InitializationException, AccessDeniedException, DatabaseException,
        SystemServiceStartException
    {
        openFlexTargetProcessManager.initialize();
    }
}
