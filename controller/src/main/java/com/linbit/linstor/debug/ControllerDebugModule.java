package com.linbit.linstor.debug;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;

import com.linbit.linstor.core.CoreModule;

import jakarta.inject.Named;
import java.util.concurrent.locks.ReadWriteLock;

public class ControllerDebugModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        Multibinder<CommonDebugCmd> commandsBinder =
            Multibinder.newSetBinder(binder(), CommonDebugCmd.class);

        commandsBinder.addBinding().to(CmdDisplayConfValue.class);
        commandsBinder.addBinding().to(CmdSetConfValue.class);
        commandsBinder.addBinding().to(CmdDeleteConfValue.class);
        commandsBinder.addBinding().to(CmdDisplayObjectStatistics.class);
    }

    @Provides
    CmdDisplayNodes cmdDisplayNodes(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        CoreModule.NodesMap nodesMap
    )
    {
        return new CmdDisplayNodes(
            reconfigurationLockRef,
            nodesMapLockRef,
            nodesMap
        );
    }

    @Provides
    CmdDisplayResource cmdDisplayResource(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CoreModule.ResourceDefinitionMap resourceDefinitionMap
    )
    {
        return new CmdDisplayResource(
            reconfigurationLockRef,
            nodesMapLockRef,
            rscDfnMapLockRef,
            resourceDefinitionMap
        );
    }

    @Provides
    CmdDisplayResourceDfn cmdDisplayResourceDfn(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CoreModule.ResourceDefinitionMap resourceDefinitionMap
    )
    {
        return new CmdDisplayResourceDfn(
            reconfigurationLockRef,
            rscDfnMapLockRef,
            resourceDefinitionMap
        );
    }

    @Provides
    CmdDisplayStorPool cmdDisplayStorPool(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        CoreModule.StorPoolDefinitionMap storPoolDefinitionMap
    )
    {
        return new CmdDisplayStorPool(
            reconfigurationLockRef,
            storPoolDfnMapLockRef,
            storPoolDefinitionMap
        );
    }

    @Provides
    CmdDisplayStorPoolDfn cmdDisplayStorPoolDfn(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        CoreModule.StorPoolDefinitionMap storPoolDefinitionMap
    )
    {
        return new CmdDisplayStorPoolDfn(
            reconfigurationLockRef,
            storPoolDfnMapLockRef,
            storPoolDefinitionMap
        );
    }
}
