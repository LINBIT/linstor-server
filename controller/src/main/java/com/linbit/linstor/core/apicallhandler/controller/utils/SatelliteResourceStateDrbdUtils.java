package com.linbit.linstor.core.apicallhandler.controller.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.layer.storage.ebs.EbsUtils;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;

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
                Resource rsc = node.getResource(accCtx, rscName);
                // list of nodes might have been created from a list of snapshot, where the corresponding
                // resource is already deleted
                if (rsc != null && !rsc.isDeleted() && !SatelliteResourceStateDrbdUtils.allVolumesUpToDate(accCtx, rsc))
                {
                    ret = false;
                    break;
                }
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
        return ret;
    }

    public static boolean allVolumesUpToDate(AccessContext accCtx, Resource rsc) throws AccessDeniedException
    {
        return allVolumesUpToDate(accCtx, rsc, true);
    }

    public static boolean allVolumesUpToDate(AccessContext accCtx, Resource rsc, boolean defaultIfUnknown)
        throws AccessDeniedException
    {
        boolean ret = defaultIfUnknown;

        boolean checkState = LayerRscUtils.getLayerStack(rsc, accCtx).contains(DeviceLayerKind.DRBD);
        // do not check EBS target resource, only initiator resource
        checkState &= (!EbsUtils.hasEbsVlms(rsc, accCtx) ||
            rsc.getStateFlags().isSet(accCtx, Resource.Flags.EBS_INITIATOR));

        checkState &= !rsc.getStateFlags().isSet(accCtx, Resource.Flags.DRBD_DISKLESS);

        if (checkState)
        {
            Peer peer = rsc.getNode().getPeer(accCtx);
            ResourceName rscName = rsc.getResourceDefinition().getName();
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
        }

        return ret;
    }
}
