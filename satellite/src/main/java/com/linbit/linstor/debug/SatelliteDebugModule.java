package com.linbit.linstor.debug;

import com.linbit.linstor.core.CoreModule;

import jakarta.inject.Named;

import java.util.concurrent.locks.ReadWriteLock;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;

public class SatelliteDebugModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        Multibinder<CommonDebugCmd> commandsBinder =
            Multibinder.newSetBinder(binder(), CommonDebugCmd.class);

        commandsBinder.addBinding().to(CmdRunDeviceManager.class);
        commandsBinder.addBinding().to(CmdAbortDeviceManager.class);
    }

    @Provides
    CmdDisplayNodes cmdDisplayNodes(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        CoreModule.NodesMap nodesMapRef
    )
    {
        return new CmdDisplayNodes(reconfigurationLockRef, nodesMapLockRef, nodesMapRef);
    }

    @Provides
    CmdDisplayResource cmdDisplayResource(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef
    )
    {
        return new CmdDisplayResource(
            reconfigurationLockRef, nodesMapLockRef, rscDfnMapLockRef, rscDfnMapRef);
    }

    @Provides
    CmdDisplayResourceDfn cmdDisplayResourceDfn(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef
    )
    {
        return new CmdDisplayResourceDfn(reconfigurationLockRef, rscDfnMapLockRef, rscDfnMapRef);
    }

    @Provides
    CmdDisplayStorPool cmdDisplayStorPool(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef
    )
    {
        return new CmdDisplayStorPool(
            reconfigurationLockRef, storPoolDfnMapLockRef, storPoolDfnMapRef);
    }

    @Provides
    CmdDisplayStorPoolDfn cmdDisplayStorPoolDfn(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef
    )
    {
        return new CmdDisplayStorPoolDfn(
            reconfigurationLockRef, storPoolDfnMapLockRef, storPoolDfnMapRef);
    }
}
