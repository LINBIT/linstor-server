package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.mgr.SnapshotRollbackManager;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinitionControllerFactory;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.makeSnapshotContext;
import static com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller.notConnectedError;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

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
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlSnapshotHelper ctrlSnapshotHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final BackupInfoManager backupInfoMgr;
    private final SnapshotRollbackManager snapRollbackMgr;
    private final CtrlPropsHelper propsHelper;

    @Inject
    public CtrlSnapshotRollbackApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSnapshotHelper ctrlSnapshotHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        BackupInfoManager backupInfoMgrRef,
        SnapshotRollbackManager snapRollbackMgrRef,
        ErrorReporter errorReporterRef,
        CtrlPropsHelper propsHelperRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSnapshotHelper = ctrlSnapshotHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        backupInfoMgr = backupInfoMgrRef;
        snapRollbackMgr = snapRollbackMgrRef;
        errorReporter = errorReporterRef;
        propsHelper = propsHelperRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
        throws AccessDeniedException
    {
        boolean anyNodeRollbackPending = false;

        Iterator<Resource> rscIter = rscDfn.iterateResource(apiCtx);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (rsc.getProps(apiCtx).map().get(ApiConsts.KEY_RSC_ROLLBACK_TARGET) != null)
            {
                anyNodeRollbackPending = true;
            }
        }

        return anyNodeRollbackPending ?
            Collections.singletonList(updateForRollback(rscDfn.getName())) :
            Collections.emptyList();
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
                "Rollback to snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> rollbackSnapshotInTransaction(rscNameStr, snapshotNameStr)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> rollbackSnapshotInTransaction(String rscNameStr, String snapshotNameStr)
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameStr, snapshotNameStr, true);
        ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();

        ensureNoBackupRestoreRunning(rscDfn);
        ensureMostRecentSnapshot(rscDfn, snapshotDfn);
        ctrlSnapshotHelper.ensureSnapshotSuccessful(snapshotDfn);
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

        Flux<ApiCallRc> nextStep = startRollback(rscName, snapshotName);

        return Flux
            .just(responses)
            .concatWith(ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, notConnectedError(), nextStep)
                .transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        updateResponses,
                        rscName,
                        "Deactivated resource {1} on {0} for rollback"
                    )
                )
                .onErrorResume(exception -> reactivateRscDfn(rscName, exception))
            )
            .concatWith(nextStep)
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
    }

    private Flux<ApiCallRc> reactivateRscDfn(ResourceName rscName, Throwable exception)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Reactivate resources due to failed deactivation",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> reactivateRscDfnInTransaction(rscName, exception)
            );
    }

    private Flux<ApiCallRc> reactivateRscDfnInTransaction(
        ResourceName rscName,
        Throwable exception
    )
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);

        unmarkDownPrivileged(rscDfn);

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> nextStep = Flux.error(exception);
        return ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, nextStep)
            // ensure that the individual node update fluxes are subscribed to, but ignore the responses
            .flatMap(Tuple2::getT2).thenMany(Flux.<ApiCallRc>empty())
            .concatWith(
                Flux.just(
                    ApiCallRcImpl.singletonApiCallRc(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.MODIFIED,
                            "Rollback of '" + rscName + "' aborted due to error deactivating"
                        )
                    )
                )
            )
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty())
            .concatWith(nextStep);
    }

    private Flux<ApiCallRc> startRollback(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Initiate rollback",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> startRollbackInTransaction(rscName, snapshotName)
            );
    }

    private Flux<ApiCallRc> startRollbackInTransaction(ResourceName rscName, SnapshotName snapshotName)
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);
        ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();

        Iterator<Resource> rscIter = iterateResourcePrivileged(rscDfn);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            getProps(rsc).map().put(ApiConsts.KEY_RSC_ROLLBACK_TARGET, snapshotName.displayValue);
        }

        ctrlTransactionHelper.commit();

        return updateForRollback(rscName);
    }

    // Restart from here when connection established and any ROLLBACK_TARGET flag set
    private Flux<ApiCallRc> updateForRollback(ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Update for rollback",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                () -> updateForRollbackInScope(rscName)
            );
    }

    private Flux<ApiCallRc> updateForRollbackInScope(ResourceName rscName)
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);

        Set<NodeName> diskNodeNames = new HashSet<>();

        Iterator<Resource> rscIter = iterateResourcePrivileged(rscDfn);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();

            if (!isDisklessPrivileged(rsc))
            {
                diskNodeNames.add(rsc.getNode().getName());
            }
        }

        Flux<ApiCallRc> finishRollback = finishRollback(rscName);
        Flux<ApiCallRc> snapRollbackFlux = snapRollbackMgr.prepareFlux(rscDfn, diskNodeNames);
        var logContextMap = MDC.getCopyOfContextMap();
        return Flux.merge(
            // both fluxes need to be started simultaneously, since the updateSatellites triggers
            // "SnapshotRollbackResult" responses that require an initialized fluxSink within the snapRollbackFlux. That
            // however only gets initialized when snapRollbackFlux is subscribed to
            ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, finishRollback)
                .map(
                    nodeResponse -> {
                        MDC.setContextMap(logContextMap);
                        return handleRollbackResponse(rscName, nodeResponse);
                    }
                )
                .transform(
                    responses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        responses,
                        rscName,
                        diskNodeNames,
                        "Rolled resource {1} back on {0}",
                        null
                    )
                ),
            snapRollbackFlux
        )
            .concatWith(finishRollback);
    }

    private Tuple2<NodeName, Flux<ApiCallRc>> handleRollbackResponse(
        ResourceName rscName,
        Tuple2<NodeName, Flux<ApiCallRc>> nodeResponse
    )
    {
        NodeName nodeName = nodeResponse.getT1();

        var logContextMap = MDC.getCopyOfContextMap();
        return nodeResponse.mapT2(responses -> responses
            .concatWith(scopeRunner
                .fluxInTransactionalScope(
                    "Handle successful rollback",
                    lockGuardFactory.create()
                        .read(LockObj.NODES_MAP)
                        .write(LockObj.RSC_DFN_MAP)
                        .buildDeferred(),
                    () -> resourceRollbackSuccessfulInTransaction(rscName, nodeName),
                    logContextMap
                )
            )
        );
    }

    private <T> Flux<T> resourceRollbackSuccessfulInTransaction(
        ResourceName rscName,
        NodeName nodeName
    )
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, true);

        getProps(rsc).map().remove(ApiConsts.KEY_RSC_ROLLBACK_TARGET);

        ctrlTransactionHelper.commit();

        return Flux.empty();
    }

    private Flux<ApiCallRc> finishRollback(ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Reactivate resources after rollback",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                () -> finishRollbackInScope(rscName)
            );
    }

    private Flux<ApiCallRc> finishRollbackInScope(ResourceName rscName)
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);
        unmarkDownPrivileged(rscDfn);

        return ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
            .transform(responses -> CtrlResponseUtils.combineResponses(
                errorReporter,
                responses,
                rscName,
                "Re-activated resource {1} on {0} after rollback"
            ));
    }

    private void ensureNoBackupRestoreRunning(ResourceDefinition rscDfn)
    {
        if (backupInfoMgr.restoreContainsRscDfn(rscDfn))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_IN_USE,
                    rscDfn.getName().displayValue + " is currently being restored from a backup. " +
                        "Please wait until the restore is finished"
                )
            );
        }
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

    private void ensureSnapshotsForAllVolumes(ResourceDefinition rscDfn, SnapshotDefinition snapshotDfn)
    {
        Iterator<Resource> rscIter = ctrlSnapshotHelper.iterateResource(rscDfn);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (!isDiskless(rsc))
            {
                Snapshot snapshot = ctrlApiDataLoader.loadSnapshot(rsc.getNode(), snapshotDfn);

                Iterator<Volume> vlmIter = rsc.iterateVolumes();
                while (vlmIter.hasNext())
                {
                    Volume vlm = vlmIter.next();

                    ctrlApiDataLoader.loadSnapshotVlm(snapshot, vlm.getVolumeDefinition().getVolumeNumber());
                }
            }
            else if (isEbsInitiator(rsc))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_IN_USE,
                        "Cannot rollback EBS volume while attached."
                    )
                        .setCorrection("Delete the EBS initiator resource(s) first")
                );
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
            NodeName nodeName = rscInUse.get().getNode().getName();
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
            AccessContext accCtx = peerAccCtx.get();
            diskless = rsc.isDrbdDiskless(accCtx) || rsc.isNvmeInitiator(accCtx) || rsc.isEbsInitiator(accCtx);
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

    private boolean isEbsInitiator(Resource rsc)
    {
        boolean ebsInit;
        try
        {
            AccessContext accCtx = peerAccCtx.get();
            ebsInit = rsc.isEbsInitiator(accCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check if " + getRscDescriptionInline(rsc) + " is an EBS initiator",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return ebsInit;
    }

    private long getMaxSequenceNumber(ResourceDefinition rscDfn)
    {
        long maxSequenceNumber;
        try
        {
            maxSequenceNumber = SnapshotDefinitionControllerFactory.maxSequenceNumber(peerAccCtx.get(), rscDfn);
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
            sequenceNumber = Long.parseLong(
                propsHelper.getProps(snapshotDfn, false).getProp(ApiConsts.KEY_SNAPSHOT_DFN_SEQUENCE_NUMBER)
            );
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
            Map<String, DrbdRscDfnData<Resource>> drbdRscDfnDataMap = rscDfn.getLayerData(
                peerAccCtx.get(),
                DeviceLayerKind.DRBD
            );
            for (DrbdRscDfnData<Resource> drbdRscDfnData : drbdRscDfnDataMap.values())
            {
                drbdRscDfnData.setDown(true);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getRscDfnDescriptionInline(rscDfn) + " down",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private boolean isDisklessPrivileged(Resource rsc)
    {
        boolean diskless;
        try
        {
            diskless = rsc.isDrbdDiskless(apiCtx) || rsc.isNvmeInitiator(apiCtx);
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
            Map<String, DrbdRscDfnData<Resource>> drbdRscDfnDataMap = rscDfn.getLayerData(
                peerAccCtx.get(),
                DeviceLayerKind.DRBD
            );
            for (DrbdRscDfnData<Resource> drbdRscDfnData : drbdRscDfnDataMap.values())
            {
                drbdRscDfnData.setDown(false);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
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

    private Props getProps(Resource rsc)
    {
        Props props;
        try
        {
            props = rsc.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "get props of " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return props;
    }
}
