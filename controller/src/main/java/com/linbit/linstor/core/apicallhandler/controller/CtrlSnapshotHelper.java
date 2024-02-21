package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.layer.storage.ebs.EbsUtils;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler.getNodeDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDfnDescriptionInline;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Iterator;

@Singleton
public class CtrlSnapshotHelper
{
    private final AccessContext apiCtx;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlSnapshotHelper(
        @ApiContext AccessContext apiCtxRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        peerAccCtx = peerAccCtxRef;
    }

    public Iterator<Resource> iterateResource(ResourceDefinition rscDfn)
    {
        Iterator<Resource> rscIter;
        try
        {
            rscIter = rscDfn.iterateResource(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "iterate the resources of " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
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
        try
        {
            if (!snapshotDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SUCCESSFUL))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "Unable to use failed snapshot"
                ));
            }

            for (Snapshot snapshot : snapshotDfn.getAllSnapshots(peerAccCtx.get()))
            {
                if (EbsUtils.isEbs(peerAccCtx.get(), snapshot) &&
                    !EbsUtils.isSnapshotCompleted(peerAccCtx.get(), snapshot))
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
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check success state of " + getSnapshotDfnDescriptionInline(snapshotDfn),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
    }

    private Peer getPeer(Node node)
    {
        Peer peer;
        try
        {
            peer = node.getPeer(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "get peer of " + getNodeDescriptionInline(node),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return peer;
    }
}
