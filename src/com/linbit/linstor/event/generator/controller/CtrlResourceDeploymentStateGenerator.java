package com.linbit.linstor.event.generator.controller;

import com.linbit.linstor.Node;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.generator.ResourceDeploymentStateGenerator;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.locks.ReadWriteLock;

@Singleton
public class CtrlResourceDeploymentStateGenerator implements ResourceDeploymentStateGenerator
{
    private final AccessContext accCtx;
    private final CoreModule.NodesMap nodesMap;
    private final ReadWriteLock nodesMapLock;

    @Inject
    public CtrlResourceDeploymentStateGenerator(
        @ApiContext AccessContext accCtxRef,
        CoreModule.NodesMap nodesMapRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef
    )
    {
        accCtx = accCtxRef;
        nodesMap = nodesMapRef;
        nodesMapLock = nodesMapLockRef;
    }

    @Override
    public ApiCallRc generate(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        ApiCallRc deploymentState = null;

        nodesMapLock.readLock().lock();
        try
        {
            Node node = nodesMap.get(objectIdentifier.getNodeName());

            if (node != null)
            {
                Peer peer = node.getPeer(accCtx);

                if (peer != null)
                {
                    deploymentState = peer.getSatelliteState().getFromResource(
                        objectIdentifier.getResourceName(),
                        SatelliteResourceState::getDeploymentState
                    );
                }
            }
        }
        finally
        {
            nodesMapLock.readLock().unlock();
        }

        return deploymentState;
    }
}
