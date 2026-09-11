package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.layer.storage.ebs.EbsUtils;
import com.linbit.linstor.netcom.Peer;


import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Iterator;

@Singleton
public class CtrlSnapshotHelper
{

    @Inject
    public CtrlSnapshotHelper(
    )
    {
    }

    public Iterator<Resource> iterateResource(ResourceDefinition rscDfn)
    {
        Iterator<Resource> rscIter;
        rscIter = rscDfn.iterateResource();
        return rscIter;
    }

    public boolean satelliteConnected(Resource rsc)
    {
        Node node = rsc.getNode();
        Peer currentPeer = getPeer(node);
        return currentPeer.isOnline();
    }

    public void ensureSatelliteConnected(Resource rsc, String details)
    {
        Node node = rsc.getNode();
        Peer currentPeer = getPeer(node);

        boolean connected = currentPeer.isOnline();
        if (!connected)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_NOT_CONNECTED,
                    "No active connection to satellite '" + node.getName() + "'."
                )
                .setDetails(details)
                .build()
            );
        }
    }

    public void ensureSnapshotSuccessful(SnapshotDefinition snapshotDfn)
    {
        if (!snapshotDfn.getFlags().isSet(SnapshotDefinition.Flags.SUCCESSFUL))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_UNKNOWN_ERROR,
                "Unable to use failed snapshot"
            ));
        }

        for (Snapshot snapshot : snapshotDfn.getAllSnapshots())
        {
            if (EbsUtils.isEbs(snapshot) &&
                !EbsUtils.isSnapshotCompleted(snapshot))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_IN_USE,
                        snapshotDfn.getName().displayValue +
                            " is not yet completed and cannot be restored right now. " +
                            "Please wait until the snapshot is completed"
                    )
                );
            }
        }
    }

    private Peer getPeer(Node node)
    {
        Peer peer;
        peer = node.getPeer();
        return peer;
    }
}
