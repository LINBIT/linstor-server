package com.linbit.linstor.systemstarter;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.core.SpecialSatelliteProcessManager;
import com.linbit.linstor.dbdrivers.DatabaseException;

import jakarta.inject.Inject;

public class SpecStltProcMgrInit implements StartupInitializer
{
    private final SpecialSatelliteProcessManager specStltTargetProcessManager;

    @Inject
    public SpecStltProcMgrInit(SpecialSatelliteProcessManager specStltTargetProcessManagerRef)
    {
        specStltTargetProcessManager = specStltTargetProcessManagerRef;
    }

    @Override
    public void initialize() throws InitializationException, DatabaseException,
        SystemServiceStartException
    {
        specStltTargetProcessManager.initialize();
    }
}
