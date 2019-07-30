package com.linbit.linstor.debug;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Named;
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
        commandsBinder.addBinding().to(CmdDisplayObjProt.class);
        commandsBinder.addBinding().to(CmdChangeObjProt.class);
    }

    // Use Provides methods because the ObjectProtection objects are not present on the satellite
    @Provides
    CmdDisplayNodes cmdDisplayNodes(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        NodeRepository nodeRepository,
        CoreModule.NodesMap nodesMap
    )
        throws AccessDeniedException
    {
        return new CmdDisplayNodes(
            reconfigurationLockRef,
            nodesMapLockRef,
            nodeRepository::getObjProt,
            nodesMap
        );
    }

    @Provides
    CmdDisplayResource cmdDisplayResource(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        ResourceDefinitionRepository resourceDefinitionRepository,
        CoreModule.ResourceDefinitionMap resourceDefinitionMap
    )
        throws AccessDeniedException
    {
        return new CmdDisplayResource(
            reconfigurationLockRef,
            nodesMapLockRef,
            rscDfnMapLockRef,
            resourceDefinitionRepository::getObjProt,
            resourceDefinitionMap
        );
    }

    @Provides
    CmdDisplayResourceDfn cmdDisplayResourceDfn(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        ResourceDefinitionRepository resourceDefinitionRepository,
        CoreModule.ResourceDefinitionMap resourceDefinitionMap
    )
        throws AccessDeniedException
    {
        return new CmdDisplayResourceDfn(
            reconfigurationLockRef,
            rscDfnMapLockRef,
            resourceDefinitionRepository::getObjProt,
            resourceDefinitionMap
        );
    }

    @Provides
    CmdDisplayStorPool cmdDisplayStorPool(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        StorPoolDefinitionRepository storPoolDefinitionRepository,
        CoreModule.StorPoolDefinitionMap storPoolDefinitionMap
    )
        throws AccessDeniedException
    {
        return new CmdDisplayStorPool(
            reconfigurationLockRef,
            storPoolDfnMapLockRef,
            storPoolDefinitionRepository::getObjProt,
            storPoolDefinitionMap
        );
    }

    @Provides
    CmdDisplayStorPoolDfn cmdDisplayStorPoolDfn(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        StorPoolDefinitionRepository storPoolDefinitionRepository,
        CoreModule.StorPoolDefinitionMap storPoolDefinitionMap
    )
        throws AccessDeniedException
    {
        return new CmdDisplayStorPoolDfn(
            reconfigurationLockRef,
            storPoolDfnMapLockRef,
            storPoolDefinitionRepository::getObjProt,
            storPoolDefinitionMap
        );
    }
}
