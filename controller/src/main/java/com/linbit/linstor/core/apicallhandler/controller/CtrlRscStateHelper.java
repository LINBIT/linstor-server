package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

@Singleton
public class CtrlRscStateHelper
{
    private final CtrlMinIoSizeHelper minIoSizeHelper;

    @Inject
    public CtrlRscStateHelper(
        CtrlMinIoSizeHelper minIoSizeHelperRef
    )
    {
        minIoSizeHelper = minIoSizeHelperRef;
    }

    public void getResourceState(
        final Resource rsc,
        final Map<NodeName, SatelliteResourceState> stateMap
    )
    {
        final Node rscNode = rsc.getNode();
        final ResourceName rscName = rsc.getResourceDefinition().getName();
        final Peer curPeer = rscNode.getPeer();
        final ReadWriteLock stltStateLock = curPeer.getSatelliteStateLock();
        final Lock stltStateRdLock = stltStateLock.readLock();
        stltStateRdLock.lock();
        try
        {
            final @Nullable SatelliteState stltState = curPeer.getSatelliteState();
            if (stltState != null)
            {
                final Map<ResourceName, SatelliteResourceState> rscStateMap = stltState.getResourceStates();
                final @Nullable SatelliteResourceState rscState = rscStateMap.get(rscName);
                if (rscState != null)
                {
                    stateMap.put(rscNode.getName(), rscState);
                }
            }
        }
        finally
        {
            stltStateRdLock.unlock();
        }
    }

    /**
     * Checks whether the resource is Primary on any node or any of its DRBD devices is open on any node, even
     * read-only on a Secondary. Either prevents taking the resource down.
     *
     * @return true if any of the given states is Primary or open
     */
    public static boolean hasPrimaryOrOpen(final @Nullable Map<NodeName, SatelliteResourceState> stateMap)
    {
        boolean resultFlag = false;
        if (stateMap != null)
        {
            final Collection<SatelliteResourceState> stateColl = stateMap.values();
            final Iterator<SatelliteResourceState> rscStateIter = stateColl.iterator();
            while (!resultFlag && rscStateIter.hasNext())
            {
                final @Nullable SatelliteResourceState rscState = rscStateIter.next();
                if (rscState != null && Boolean.TRUE.equals(rscState.isInUseOrOpen()))
                {
                    resultFlag = true;
                }
            }
        } // end if (stateMap != null)
        return resultFlag;
    }

    public boolean canChangeMinIoSize(@Nullable ResourceDefinition rscDfnRef)
    {
        boolean ret = rscDfnRef == null || rscDfnRef.getResourceCount() == 0;
        if (rscDfnRef != null)
        {
            if (minIoSizeHelper.isAutoMinIoSize(rscDfnRef))
            {
                Map<NodeName, SatelliteResourceState> rscStateMap = new TreeMap<>();
                Iterator<Resource> rscIter = rscDfnRef.iterateResource();
                while (rscIter.hasNext())
                {
                    final Resource rsc = rscIter.next();
                    getResourceState(rsc, rscStateMap);
                }
                // Can change the minimum I/O size by restarting resources after changing the configuration
                // if there is no primary peer and no peer has the device open
                ret = !CtrlRscStateHelper.hasPrimaryOrOpen(rscStateMap);

                if (ret)
                {
                    // Can change the minimum I/O size if no volume-definition's block-size is already frozen
                    // (i.e. due to the resource being primary already)
                    Iterator<VolumeDefinition> vlmDfnIt = rscDfnRef.iterateVolumeDfn();
                    while (vlmDfnIt.hasNext() && ret)
                    {
                        VolumeDefinition vlmDfn = vlmDfnIt.next();
                        @Nullable String freezeValue = vlmDfn.getProps()
                            .getProp(ApiConsts.KEY_DRBD_FREEZE_BLOCK_SIZE, ApiConsts.NAMESPC_LINSTOR_DRBD);
                        ret = !Boolean.valueOf(freezeValue);
                    }
                }
            }
        }
        return ret;
    }
}
