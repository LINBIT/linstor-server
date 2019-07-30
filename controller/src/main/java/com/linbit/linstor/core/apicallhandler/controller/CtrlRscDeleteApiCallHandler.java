package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceData;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

@Singleton
public class CtrlRscDeleteApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlRscDeleteApiHelper ctrlRscDeleteApiHelper;
    private final ResponseConverter responseConverter;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlRscDeleteApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlRscDeleteApiHelper ctrlRscDeleteApiHelperRef,
        ResponseConverter responseConverterRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlRscDeleteApiHelper = ctrlRscDeleteApiHelperRef;
        responseConverter = responseConverterRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        peerAccCtx = peerAccCtxRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        Iterator<Resource> rscIter = rscDfn.iterateResource(apiCtx);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (!rsc.getAssignedNode().getFlags().isSet(apiCtx, Node.NodeFlag.DELETE) &&
                !rscDfn.getFlags().isSet(apiCtx, ResourceDefinition.RscDfnFlags.DELETE) &&
                rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DELETE))
            {
                fluxes.add(ctrlRscDeleteApiHelper.updateSatellitesForResourceDelete(
                    rsc.getAssignedNode().getName(), rscDfn.getName()));
            }
        }

        return fluxes;
    }

    /**
     * Deletes a {@link Resource}.
     * <p>
     * The {@link Resource} is only deleted once the corresponding satellite confirmed
     * that it has undeployed (deleted) the {@link Resource}
     */
    public Flux<ApiCallRc> deleteResource(String nodeNameStr, String rscNameStr)
    {
        ResponseContext context = CtrlRscApiCallHandler.makeRscContext(
            ApiOperation.makeDeleteOperation(),
            nodeNameStr,
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Delete resource",
                LockGuard.createDeferred(nodesMapLock.writeLock(), rscDfnMapLock.writeLock()),
                () -> deleteResourceInTransaction(nodeNameStr, rscNameStr)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteResourceInTransaction(String nodeNameStr, String rscNameStr)
    {
        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeNameStr, rscNameStr, false);

        if (rsc == null)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_NOT_FOUND,
                getRscDescription(nodeNameStr, rscNameStr) + " not found."
            ));
        }

        NodeName nodeName = rsc.getAssignedNode().getName();
        ResourceName rscName = rsc.getDefinition().getName();

        ctrlRscDeleteApiHelper.ensureNotInUse(rsc);

        ensureNotLastDisk(rscName, rsc);

        failIfDependentSnapshot(rsc);

        ctrlRscDeleteApiHelper.markDeletedWithVolumes(rsc);

        ctrlTransactionHelper.commit();

        String descriptionFirstLetterCaps = firstLetterCaps(getRscDescription(rsc));
        ApiCallRc responses = ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
            .entryBuilder(
                ApiConsts.DELETED,
                descriptionFirstLetterCaps + " marked for deletion."
            )
            .setDetails(descriptionFirstLetterCaps + " UUID is: " + rsc.getUuid())
            .build()
        );

        return Flux
            .just(responses)
            .concatWith(ctrlRscDeleteApiHelper.updateSatellitesForResourceDelete(nodeName, rscName));
    }

    private void ensureNotLastDisk(ResourceName rscName, Resource rsc)
    {
        try
        {
            if (!rsc.isDiskless(peerAccCtx.get()) &&
                rsc.getDefinition().hasDiskless(peerAccCtx.get()) &&
                rsc.getDefinition().diskfullCount(peerAccCtx.get()) == 1)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_IN_USE,
                        String.format(
                            "Last resource of '%s' with disk still has diskless resources attached.", rscName)
                    )
                    .setCause("Resource still has diskless users.")
                    .setCorrection("Before deleting this resource, delete the diskless resources attached to it.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check whether is last with disk " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
    }

    private void failIfDependentSnapshot(ResourceData rsc)
    {
        try
        {
            for (SnapshotDefinition snapshotDfn : rsc.getDefinition().getSnapshotDfns(peerAccCtx.get()))
            {
                Snapshot snapshot = snapshotDfn.getSnapshot(peerAccCtx.get(), rsc.getAssignedNode().getName());
                if (snapshot != null)
                {
                    for (SnapshotVolume snapshotVlm : snapshot.getAllSnapshotVolumes(peerAccCtx.get()))
                    {
                        StorPool storPool = snapshotVlm.getStorPool(apiCtx);
                        if (storPool.getDeviceProviderKind().isSnapshotDependent())
                        {
                            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_EXISTS_SNAPSHOT,
                                "Resource '" + rsc.getDefinition().getName() + "' cannot be deleted because volume " +
                                    snapshotVlm.getVolumeNumber() + " has dependent snapshot '" +
                                    snapshot.getSnapshotName() + "'"
                                ));
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check for dependent snapshots of " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
    }
}
