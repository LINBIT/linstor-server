package com.linbit.linstor.core.apicallhandler.controller.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.util.Collection;

public class SatelliteResourceStateDrbdUtils
{
    public static boolean allResourcesUpToDate(Collection<Node> nodes, ResourceName rscName, AccessContext accCtx)
    {
        boolean ret = true;
        for (Node node : nodes)
        {
            try
            {
                if (!SatelliteResourceStateDrbdUtils.allVolumesUpToDate(node.getPeer(accCtx), rscName))
                {
                    ret = false;
                }
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
        return ret;
    }

    public static boolean allVolumesUpToDate(Peer peer, ResourceName rscName)
    {
        return allVolumesUpToDate(peer, rscName, true);
    }

    public static boolean allVolumesUpToDate(Peer peer, ResourceName rscName, boolean defaultIfUnknown)
    {
        boolean ret = defaultIfUnknown;

        SatelliteState stltState = peer.getSatelliteState();
        if (stltState != null)
        {
            SatelliteResourceState rscState = stltState.getResourceStates().get(rscName);
            if (rscState != null)
            {
                Collection<SatelliteVolumeState> vlmStates = rscState.getVolumeStates().values();
                if (vlmStates != null)
                {
                    ret = true;
                    for (SatelliteVolumeState stltVlmStates : vlmStates)
                    {
                        if (!stltVlmStates.getDiskState().equalsIgnoreCase("uptodate"))
                        {
                            ret = false;
                            break;
                        }
                    }
                }
            }
        }
        return ret;
    }
}
