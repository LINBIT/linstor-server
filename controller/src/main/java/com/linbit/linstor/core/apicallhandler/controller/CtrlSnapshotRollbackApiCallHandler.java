package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotDefinitionData;
import com.linbit.linstor.SnapshotDefinitionDataControllerFactory;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.Volume;
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
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSatelliteUpdateCaller.notConnectedError;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.makeSnapshotContext;
import static com.linbit.utils.StringUtils.firstLetterCaps;

/**
 * Rolls a resource back to a snapshot state.
 * <p>
 * Rollback proceeds as follows:
 * <ol>
 *     <li>The resource definition is marked as down with a transient flag. If any satellites fail to apply this
 *         update, the flag is unset and the rollback is aborted without taking effect.</li>
 *     <li>Once all the resources are down, all the resources are marked for rollback to the given snapshot.
 *         These flags are persistent and will prevent the resource from being brought up until the rollback is
 *         successful.</li>
 *     <li>For each resource that successfully applies the rollback, the flags are removed, so that it can be
 *         brought up. For resources that fail to apply the rollback, the flags remain, so that the resource
 *         remains down.</li>
 *     <li>Once all the satellites have responded to the rollback request, they are updated. This causes the
 *         resource to be brought up on all nodes where the rollback succeeded.</li>
 * </ol>
 */
@Singleton
public class CtrlSnapshotRollbackApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlSnapshotHelper ctrlSnapshotHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlSnapshotRollbackApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSnapshotHelper ctrlSnapshotHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSnapshotHelper = ctrlSnapshotHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
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

        for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns(apiCtx))
        {
            boolean anyNodeRollbackPending = false;
            for (Snapshot snapshot : snapshotDfn.getAllSnapshots(apiCtx))
            {
                if (snapshot.getFlags().isSet(apiCtx, Snapshot.SnapshotFlags.ROLLBACK_TARGET))
                {
                    anyNodeRollbackPending = true;
                }
            }

            if (anyNodeRollbackPending)
            {
                fluxes.add(startRollback(rscDfn.getName(), snapshotDfn.getName()));
            }
        }

        return fluxes;
    }

    public Flux<ApiCallRc> rollbackSnapshot(String rscNameStr, String snapshotNameStr)
    {
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeModifyOperation(),
            Collections.emptyList(),
            rscNameStr,
            snapshotNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                LockGuard.createDeferred(nodesMapLock.readLock(), rscDfnMapLock.writeLock()),
                () -> rollbackSnapshotInTransaction(rscNameStr, snapshotNameStr)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> rollbackSnapshotInTransaction(String rscNameStr, String snapshotNameStr)
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameStr, snapshotNameStr, true);
        ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();

        ensureMostRecentSnapshot(rscDfn, snapshotDfn);
        ensureSnapshotSuccessful(snapshotDfn);
        ensureSnapshotsForAllVolumes(rscDfn, snapshotDfn);
        ensureAllSatellitesConnected(rscDfn);
        ensureNoResourcesInUse(rscDfn);

        markDown(rscDfn);

        ctrlTransactionHelper.commit();

        ResourceName rscName = snapshotDfn.getResourceName();
        SnapshotName snapshotName = snapshotDfn.getName();

        ApiCallRc responses = ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl.simpleEntry(
            ApiConsts.MODIFIED,
            firstLetterCaps(getSnapshotDfnDescriptionInline(rscName, snapshotName)) + " marked down for rollback."
        ));

        Flux<ApiCallRc> updateResponsesRscDfn =
            ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, notConnectedError())
                .transform(updateResponses -> ResponseUtils.translateDeploymentSuccess(
                    updateResponses,
                    "Deactivated resource {1} on {0} for rollback"
                ));

        return Flux
            .just(responses)
            .concatWith(updateResponsesRscDfn)
            .concatWith(startRollback(rscName, snapshotName))
            .onErrorResume(exception -> reactivateRscDfn(rscName, exception))
            .onErrorResume(CtrlSatelliteUpdateCaller.DelayedApiRcException.class, ignored -> Flux.empty());
    }

    private Flux<ApiCallRc> reactivateRscDfn(ResourceName rscName, Throwable exception)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                LockGuard.createDeferred(nodesMapLock.readLock(), rscDfnMapLock.writeLock()),
                () -> reactivateRscDfnInTransaction(rscName, exception)
            );
    }

    private Flux<ApiCallRc> reactivateRscDfnInTransaction(
        ResourceName rscName,
        Throwable exception
    )
    {
        ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);

        unmarkDownPrivileged(rscDfn);

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses =
            ctrlSatelliteUpdateCaller.updateSatellites(rscDfn)
                .thenMany(Flux.<ApiCallRc>empty())
                .concatWith(Flux.just(ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl.simpleEntry(
                    ApiConsts.MODIFIED,
                    "Rollback of '" + rscName + "' aborted due to error deactivating"
                ))))
                .onErrorResume(CtrlSatelliteUpdateCaller.DelayedApiRcException.class, ignored -> Flux.empty());

        return satelliteUpdateResponses
            .concatWith(Flux.error(exception));
    }

    private Flux<ApiCallRc> startRollback(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                LockGuard.createDeferred(nodesMapLock.readLock(), rscDfnMapLock.writeLock()),
                () -> startRollbackInTransaction(rscName, snapshotName)
            );
    }

    private Flux<ApiCallRc> startRollbackInTransaction(ResourceName rscName, SnapshotName snapshotName)
    {
        SnapshotDefinitionData snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);
        ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();

        unmarkDownPrivileged(rscDfn);

        Iterator<Resource> rscIter = iterateResourcePrivileged(rscDfn);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            markInRollbackPrivileged(rsc, true);

            if (!isDisklessPrivileged(rsc))
            {
                Snapshot snapshot = getSnapshotPrivileged(snapshotDfn, rsc.getAssignedNode().getName());
                markRollbackTargetPrivileged(snapshot, true);
            }
        }

        ctrlTransactionHelper.commit();

        return updateForRollback(rscName, snapshotName);
    }

    // Restart from here when connection established and any ROLLBACK_TARGET flag set
    private Flux<ApiCallRc> updateForRollback(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                LockGuard.createDeferred(nodesMapLock.readLock(), rscDfnMapLock.readLock()),
                () -> updateForRollbackInScope(rscName, snapshotName)
            );
    }

    private Flux<ApiCallRc> updateForRollbackInScope(ResourceName rscName, SnapshotName snapshotName)
    {
        SnapshotDefinitionData snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);
        ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();

        // Send the IN_ROLLBACK resource flag
        Flux<ApiCallRc> updateResponsesRscDfn =
            ctrlSatelliteUpdateCaller.updateSatellites(rscDfn)
                .thenMany(Flux.empty());

        // Send the ROLLBACK_TARGET snapshot flag
        Flux<ApiCallRc> updateResponsesSnapshotDfn =
            ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, CtrlSatelliteUpdateCaller.notConnectedWarn())
                .flatMap(nodeResponse -> handleRollbackResponse(
                    rscName,
                    snapshotName,
                    nodeResponse
                ))
                .transform(responses -> ResponseUtils.translateDeploymentSuccess(
                    responses,
                    "Rolled resource {1} back to snapshot ''" + snapshotName + "'' on {0}"
                ));

        return updateResponsesRscDfn
            .concatWith(updateResponsesSnapshotDfn)
            .concatWith(finishRollback(rscName, snapshotName));
    }

    private Flux<Tuple2<NodeName, ApiCallRc>> handleRollbackResponse(
        ResourceName rscName,
        SnapshotName snapshotName,
        Tuple2<NodeName, ApiCallRc> nodeResponse
    )
    {
        NodeName nodeName = nodeResponse.getT1();
        ApiCallRc response = nodeResponse.getT2();

        boolean responseIncludesSuccess = response.getEntries().stream()
            .anyMatch(rcEntry -> rcEntry.getReturnCode() == ApiConsts.MODIFIED);
        boolean responseIncludesNonSuccess = response.getEntries().stream()
            .anyMatch(rcEntry -> rcEntry.getReturnCode() != ApiConsts.MODIFIED);

        Flux<Tuple2<NodeName, ApiCallRc>> markSuccessfulFlux = responseIncludesSuccess && !responseIncludesNonSuccess ?
            scopeRunner
                .fluxInTransactionalScope(
                    LockGuard.createDeferred(nodesMapLock.readLock(), rscDfnMapLock.writeLock()),
                    () -> resourceRollbackSuccessfulInTransaction(rscName, snapshotName, nodeName)
                ) :
            Flux.empty();

        return markSuccessfulFlux
            .concatWith(Flux.just(nodeResponse));
    }

    private <T> Flux<T> resourceRollbackSuccessfulInTransaction(
        ResourceName rscName,
        SnapshotName snapshotName,
        NodeName nodeName
    )
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, true);
        markInRollbackPrivileged(rsc, false);

        if (!isDisklessPrivileged(rsc))
        {
            Snapshot snapshot = ctrlApiDataLoader.loadSnapshot(
                ctrlApiDataLoader.loadNode(nodeName, true),
                ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true)
            );

            markRollbackTargetPrivileged(snapshot, false);
        }

        ctrlTransactionHelper.commit();

        return Flux.empty();
    }

    private Flux<ApiCallRc> finishRollback(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                LockGuard.createDeferred(nodesMapLock.readLock(), rscDfnMapLock.writeLock()),
                () -> finishRollbackInTransaction(rscName, snapshotName)
            );
    }

    private Flux<ApiCallRc> finishRollbackInTransaction(ResourceName rscName, SnapshotName snapshotName)
    {
        SnapshotDefinitionData snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);
        ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();

        Iterator<Resource> rscIter = iterateResourcePrivileged(rscDfn);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (isDisklessPrivileged(rsc))
            {
                markInRollbackPrivileged(rsc, false);
            }
        }

        ctrlTransactionHelper.commit();

        // Remove the successful in-progress snapshots from the satellites
        Flux<ApiCallRc> updateResponsesSnapshotDfn =
            ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, CtrlSatelliteUpdateCaller.notConnectedWarn())
                .thenMany(Flux.empty());

        // Re-activate the successful resources
        Flux<ApiCallRc> updateResponsesRscDfn =
            ctrlSatelliteUpdateCaller.updateSatellites(rscDfn)
                .transform(responses -> ResponseUtils.translateDeploymentSuccess(
                    responses,
                    "Re-activated resource {1} on {0} after rollback"
                ));

        return updateResponsesSnapshotDfn
            .concatWith(updateResponsesRscDfn);
    }

    private void ensureMostRecentSnapshot(ResourceDefinition rscDfn, SnapshotDefinition snapshotDfn)
    {
        // Rollback is only supported with the most recent snapshot.
        // In principle, we could allow rollback to earlier snapshots, however, it makes the situation complicated,
        // especially with ZFS.
        // When performing a rollback with ZFS, the intermediate snapshots are destroyed.
        // The user can always delete the intermediate snapshots and then roll back if they wish to use an earlier
        // snapshot.
        long maxSequenceNumber = getMaxSequenceNumber(rscDfn);
        long sequenceNumber = getSequenceNumber(snapshotDfn);
        if (sequenceNumber != maxSequenceNumber)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN,
                "Rollback is only allowed with the most recent snapshot"
            ));
        }
    }

    private void ensureSnapshotSuccessful(SnapshotDefinition snapshotDfn)
    {
        try
        {
            if (!snapshotDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.SnapshotDfnFlags.SUCCESSFUL))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "Unable to roll back to failed snapshot"
                ));
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

    private void ensureSnapshotsForAllVolumes(ResourceDefinition rscDfn, SnapshotDefinition snapshotDfn)
    {
        Iterator<Resource> rscIter = ctrlSnapshotHelper.iterateResource(rscDfn);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (!isDiskless(rsc))
            {
                Snapshot snapshot = ctrlApiDataLoader.loadSnapshot(rsc.getAssignedNode(), snapshotDfn);

                Iterator<Volume> vlmIter = rsc.iterateVolumes();
                while (vlmIter.hasNext())
                {
                    Volume vlm = vlmIter.next();

                    ctrlApiDataLoader.loadSnapshotVlm(snapshot, vlm.getVolumeDefinition().getVolumeNumber());
                }
            }
        }
    }

    private void ensureAllSatellitesConnected(ResourceDefinition rscDfn)
    {
        Iterator<Resource> rscIter = ctrlSnapshotHelper.iterateResource(rscDfn);
        while (rscIter.hasNext())
        {
            ctrlSnapshotHelper.ensureSatelliteConnected(
                rscIter.next(),
                "Snapshot rollback cannot be performed when the corresponding satellites are not connected."
            );
        }
    }

    private void ensureNoResourcesInUse(ResourceDefinition rscDfn)
    {
        Optional<Resource> rscInUse = anyResourceInUse(rscDfn);
        if (rscInUse.isPresent())
        {
            NodeName nodeName = rscInUse.get().getAssignedNode().getName();
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_IN_USE,
                    String.format("Resource '%s' on node '%s' is still in use.", rscDfn.getName(), nodeName)
                )
                .setCause("Resource is mounted/in use.")
                .setCorrection(String.format("Un-mount resource '%s' on the node '%s'.", rscDfn.getName(), nodeName))
                .build()
            );
        }
    }

    private boolean isDiskless(Resource rsc)
    {
        boolean diskless;
        try
        {
            diskless = rsc.isDiskless(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check diskless state of " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return diskless;
    }

    private long getMaxSequenceNumber(ResourceDefinition rscDfn)
    {
        long maxSequenceNumber;
        try
        {
            maxSequenceNumber = SnapshotDefinitionDataControllerFactory.maxSequenceNumber(peerAccCtx.get(), rscDfn);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check sequence numbers of " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        return maxSequenceNumber;
    }

    private long getSequenceNumber(SnapshotDefinition snapshotDfn)
    {
        long sequenceNumber;
        try
        {
            sequenceNumber = Long.parseLong(getProps(snapshotDfn).getProp(ApiConsts.KEY_SNAPSHOT_DFN_SEQUENCE_NUMBER));
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Internal property not valid", exc);
        }
        catch (NumberFormatException exc)
        {
            throw new ImplementationError(
                "Unable to parse internal value of internal property " +
                    ApiConsts.KEY_SNAPSHOT_DFN_SEQUENCE_NUMBER,
                exc
            );
        }
        return sequenceNumber;
    }

    private Optional<Resource> anyResourceInUse(ResourceDefinition rscDfn)
    {
        Optional<Resource> rscInUse;
        try
        {
            rscInUse = rscDfn.anyResourceInUse(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check in-use state of " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        return rscInUse;
    }

    private void markDown(ResourceDefinition rscDfn)
    {
        try
        {
            rscDfn.setDown(peerAccCtx.get(), true);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getRscDfnDescriptionInline(rscDfn) + " down",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private boolean isDisklessPrivileged(Resource rsc)
    {
        boolean diskless;
        try
        {
            diskless = rsc.isDiskless(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return diskless;
    }

    private void unmarkDownPrivileged(ResourceDefinition rscDfn)
    {
        try
        {
            rscDfn.setDown(peerAccCtx.get(), false);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void markInRollbackPrivileged(Resource rsc, boolean inRollback)
    {
        try
        {
            if (inRollback)
            {
                rsc.getStateFlags().enableFlags(apiCtx, Resource.RscFlags.IN_ROLLBACK);
            }
            else
            {
                rsc.getStateFlags().disableFlags(apiCtx, Resource.RscFlags.IN_ROLLBACK);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void markRollbackTargetPrivileged(Snapshot snapshot, boolean rollbackTarget)
    {
        try
        {
            if (rollbackTarget)
            {
                snapshot.getFlags().enableFlags(apiCtx, Snapshot.SnapshotFlags.ROLLBACK_TARGET);
            }
            else
            {
                snapshot.getFlags().disableFlags(apiCtx, Snapshot.SnapshotFlags.ROLLBACK_TARGET);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private Snapshot getSnapshotPrivileged(SnapshotDefinitionData snapshotDfn, NodeName nodeName)
    {
        Snapshot snapshot;
        try
        {
            snapshot = snapshotDfn.getSnapshot(apiCtx, nodeName);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return snapshot;
    }

    private Iterator<Resource> iterateResourcePrivileged(ResourceDefinition rscDfn)
    {
        Iterator<Resource> rscIter;
        try
        {
            rscIter = rscDfn.iterateResource(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return rscIter;
    }

    private Props getProps(SnapshotDefinition snapshotDfn)
    {
        Props props;
        try
        {
            props = snapshotDfn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "get props of " + getSnapshotDfnDescriptionInline(snapshotDfn),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        return props;
    }
}
