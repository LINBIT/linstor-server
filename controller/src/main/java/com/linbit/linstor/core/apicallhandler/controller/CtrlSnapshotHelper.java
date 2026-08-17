package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.ebs.EbsStatusManagerService;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.layer.storage.ebs.EbsUtils;
import com.linbit.linstor.netcom.Peer;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collections;
import java.util.Iterator;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlSnapshotHelper
{
    private final EbsStatusManagerService ebsStatusManagerService;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlApiDataLoader ctrlApiDataLoader;

    @Inject
    public CtrlSnapshotHelper(
        EbsStatusManagerService ebsStatusManagerServiceRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef
    )
    {
        ebsStatusManagerService = ebsStatusManagerServiceRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
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

    public Flux<ApiCallRc> refreshEbsSnapStateIfNeededFlux(String rscNameStrRef, String snapNameStrRef)
    {
        return scopeRunner.fluxInTransactionlessScope(
            "Refresh EBS snapshot state if needed",
            lockGuardFactory.buildDeferred(LockType.READ, LockObj.RSC_DFN_MAP),
            () ->
            {
                Flux<ApiCallRc> ret = Flux.empty();
                @Nullable SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfnOrNull(
                    rscNameStrRef,
                    snapNameStrRef
                );
                if (snapDfn != null && hasAnyNotRestorableEbsSnapshot(snapDfn))
                {
                    ret = ebsStatusManagerService.pollFlux(
                        EbsStatusManagerService.DFLT_POLL_WAIT,
                        null,
                        Collections.singleton(snapDfn.getSnapDfnKey())
                    );
                }
                return ret;
            }
        );
    }

    public boolean hasAnyNotRestorableEbsSnapshot(SnapshotDefinition snapDfnRef)
    {
        boolean ret = false;
        for (Snapshot snapshot : snapDfnRef.getAllSnapshots())
        {
            if (EbsUtils.isEbs(snapshot))
            {
                boolean snapComplete = EbsUtils.isSnapshotRestorable(snapshot);
                if (!snapComplete)
                {
                    ret = true;
                    break;
                }
            }
        }
        return ret;
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
                !EbsUtils.isSnapshotRestorable(snapshot))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_IN_USE,
                        snapshotDfn.getName().displayValue +
                            " is not yet completed and can therefore not be restored right " +
                            "now. Please wait until the snapshot reaches 'completed' state."
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
