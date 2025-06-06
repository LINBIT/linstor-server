package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
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
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.tasks.ScheduleBackupService;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.makeSnapshotContext;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

/**
 * Rolls a resource back to a snapshot state.
 * <p>
 *      Rollback proceeds as follows:
 *      <ol>
 *          <li>Create a SAFETY_SNAP snapshot on all nodes with a diskful resource.
 *              <p>If this step fails, the SAFETY_SNAP is removed again.</p>
 *          </li>
 *          <li>All resources will be deleted (via {@link CtrlRscDfnTruncateApiCallHandler}, i.e. diskless first)
 *              <p>If this step fails, SAFTEY_SNAP will be restored on the diskful nodes that were successfully deleted.
 *                  Other resources are undeleted.</p>
 *          </li>
 *          <li>The given snapshot will be restored on the nodes that have the snapshot (which might be different nodes
 *              then we just deleted the resources from)
 *              <p>If this step fails, all existing resources (i.e. those which successfully performed a resource, if
 *                  any) will be deleted an all nodes will restore back to SAFETY_SNAP.</p>
 *          <li>SAFETY_SNAP will be deleted</li>
 *      </ol>
 * </p>
 */
@Singleton
public class CtrlSnapshotRollbackApiCallHandler implements CtrlSatelliteConnectionListener
{
    private static final String SAFETY_SNAP_PREFIX = "safety-snap-";

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
    private final CtrlSnapshotCrtApiCallHandler ctrlSnapshotCrtHandler;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapDelHandler;
    private final CtrlRscDfnTruncateApiCallHandler ctrlRscDfnTruncateApiCallHandler;
    private final ScheduleBackupService scheduleService;
    private final CtrlSnapshotRestoreApiCallHandler ctrlSnapRstApiCallHandler;
    private final CtrlSnapshotCrtHelper ctrlSnapCrtHelper;
    private final CtrlRscMakeAvailableApiCallHandler ctrlRscMakeAvailableApiCallHandler;

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
        CtrlSnapshotCrtApiCallHandler ctrlSnapshotCrtHandlerRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDelHandlerRef,
        CtrlRscDfnTruncateApiCallHandler ctrlRscDfnTruncateApiCallHandlerRef,
        ScheduleBackupService scheduleServiceRef,
        CtrlSnapshotRestoreApiCallHandler ctrlSnapRstApiCallHandlerRef,
        CtrlSnapshotCrtHelper ctrlSnapCrtHelperRef,
        CtrlRscMakeAvailableApiCallHandler ctrlRscMakeAvailableApiCallHandlerRef
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
        ctrlSnapshotCrtHandler = ctrlSnapshotCrtHandlerRef;
        ctrlSnapDelHandler = ctrlSnapDelHandlerRef;
        ctrlRscDfnTruncateApiCallHandler = ctrlRscDfnTruncateApiCallHandlerRef;
        scheduleService = scheduleServiceRef;
        ctrlSnapRstApiCallHandler = ctrlSnapRstApiCallHandlerRef;
        ctrlSnapCrtHelper = ctrlSnapCrtHelperRef;
        ctrlRscMakeAvailableApiCallHandler = ctrlRscMakeAvailableApiCallHandlerRef;
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

        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();
        if (anyNodeRollbackPending)
        {
            fluxes.add(updateForRollback(rscDfn.getName()));
        }

        for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns(apiCtx))
        {
            if (snapshotDfn.getFlags().isSet(apiCtx, SnapshotDefinition.Flags.SAFETY_SNAPSHOT)
                && snapshotDfn.getFlags().isUnset(apiCtx, SnapshotDefinition.Flags.DELETE))
            {
                fluxes.add(recoverFailedRollback(rscDfn, snapshotDfn));
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
                "prepare rollback",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> prepareRollbackInTransaction(rscNameStr, snapshotNameStr)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> prepareRollbackInTransaction(String rscNameStr, String snapshotNameStr)
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameStr, snapshotNameStr, true);
        ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();

        ResourceName rscName = rscDfn.getName();
        SnapshotName snapName = snapshotDfn.getName();
        ensureNoBackupRestoreRunning(rscDfn);
        ensureNoScheduleActive(rscNameStr);
        ctrlSnapshotHelper.ensureSnapshotSuccessful(snapshotDfn);
        ensureAllSatellitesConnected(rscDfn);
        ensureNoResourcesInUse(rscDfn);
        Map<NodeName, Boolean> rscNodes = currentRscNodeDisks(rscDfn);

        ApiCallRcImpl responses = new ApiCallRcImpl();
        try
        {
            SnapshotDefinition safetySnapDfn = ctrlSnapCrtHelper.createSnapshots(
                Collections.emptyList(),
                rscName,
                new SnapshotName(SAFETY_SNAP_PREFIX + UUID.randomUUID()),
                responses
            );
            safetySnapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SAFETY_SNAPSHOT);
            ctrlTransactionHelper.commit();
            return ctrlSnapshotCrtHandler.postCreateSnapshot(safetySnapDfn, false)
                .concatWith(Flux.<ApiCallRc>just(responses))
                .concatWith(
                    deleteRscs(rscDfn.getName())
                        .onErrorResume(
                            exc -> rollbackToSafetySnap(snapshotDfn, false).concatWith(Flux.error(exc))
                        )
                )
                .concatWith(
                    restoreSnap(rscName, snapName)
                        .onErrorResume(
                            exc -> rollbackToSafetySnap(snapshotDfn, true).concatWith(Flux.error(exc))
                        )
                )
                .concatWith(deleteSafetySnap(null, rscDfn))
                .concatWith(recreateResources(rscNameStr, rscNodes))
                .onErrorResume(exc -> deleteSafetySnap(exc, rscDfn));
        }
        catch (AccessDeniedException accDenyExc)
        {
            throw new ApiAccessDeniedException(
                accDenyExc,
                "unable to access snapshots for " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_SNAP_DFN
            );
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Flux<ApiCallRc> recoverFailedRollback(ResourceDefinition rscDfn, SnapshotDefinition snapshotDfn)
    {
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeModifyOperation(),
            Collections.emptyList(),
            rscDfn.getName().displayValue,
            snapshotDfn.getName().displayValue
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "prepare rollback",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> recoverFailedRollbackInTransaction(rscDfn, snapshotDfn)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> recoverFailedRollbackInTransaction(
        ResourceDefinition rscDfn, SnapshotDefinition snapshotDfn)
    {
        String rscNameStr = rscDfn.getName().displayValue;
        Map<NodeName, Boolean> rscNodes = currentRscNodeDisks(rscDfn);
        return rollbackToSafetySnap(snapshotDfn, true)
            .concatWith(deleteSafetySnap(null, rscDfn))
            .concatWith(recreateResources(rscNameStr, rscNodes))
            .onErrorResume(exc -> deleteSafetySnap(exc, rscDfn));
    }

    private void ensureNoScheduleActive(String rscNameStr)
    {
        if (!scheduleService.getAllFilteredActiveShippings(rscNameStr, null, null).isEmpty())
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_IN_USE,
                    rscNameStr + " has an active schedule. " +
                        "Please deactivate the schedule first before trying again."
                )
            );
        }
    }

    private Flux<ApiCallRc> deleteRscs(ResourceName rscName)
    {
        return ctrlRscDfnTruncateApiCallHandler.truncateRscDfnInTransaction(rscName, false);
    }

    private Flux<ApiCallRc> restoreSnap(ResourceName rscNameStr, SnapshotName snapNameStr)
    {
        return ctrlSnapRstApiCallHandler.restoreSnapshot(
            Collections.emptyList(),
            rscNameStr,
            snapNameStr,
            rscNameStr,
            Collections.emptyMap()
        );
    }

    private Flux<ApiCallRc> rollbackToSafetySnap(SnapshotDefinition snapDfn, boolean restoreStarted)
    {
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeModifyOperation(),
            Collections.emptyList(),
            snapDfn.getResourceName().displayValue,
            snapDfn.getName().displayValue
        );
        return scopeRunner
            .fluxInTransactionalScope(
                "rollback rollback",
                lockGuardFactory.create()
                    .read(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> rollbackToSafetySnapInTransaction(snapDfn, restoreStarted)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private @Nullable SnapshotDefinition findSafetySnapDfn(ResourceDefinition rscDfn)
    {
        @Nullable SnapshotDefinition snapDfnRet = null;
        try
        {
            for (var snapDfn : rscDfn.getSnapshotDfns(peerAccCtx.get()))
            {
                if (snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SAFETY_SNAPSHOT))
                {
                    snapDfnRet = snapDfn;
                    break;
                }
            }
        }
        catch (AccessDeniedException accDenyExc)
        {
            throw new ApiAccessDeniedException(
                accDenyExc,
                "unable to access snapshots for " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }

        return snapDfnRet;
    }

    /**
     * Performs a rollback to safety-snapshot for this resource definition.
     *
     * @param snapDfn The target snapshot definition that should have been rolled back to (*not* the SAFETY_SNAP!)
     * @param restoreStarted Indicates whether or not the restore to the given target snapDfn was already attempted.
     *      If false, the resource could not be deleted on some nodes. On those nodes, we simply restore to
     *      SAFETY_SNAP.<br>
     *      If true, we already tried to restore to {@code snapDfn} but some nodes failed to restore. In this case we
     *      will delete all successfully restored resources and perform a restore snapshot to SAFTEY_SNAP on all
     *      participating nodes.
     * @return
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    private Flux<ApiCallRc> rollbackToSafetySnapInTransaction(SnapshotDefinition snapDfn, boolean restoreStarted)
        throws AccessDeniedException, DatabaseException
    {
        ResourceDefinition rscDfn = snapDfn.getResourceDefinition();
        ResourceName rscName = rscDfn.getName();
        Flux<ApiCallRc> flux;

        @Nullable SnapshotDefinition safetySnapDfn = findSafetySnapDfn(rscDfn);

        if (safetySnapDfn == null)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN,
                "Couldn't find safety-snapshot for resource: " + rscName));
        }

        if (restoreStarted)
        {
            flux = deleteRscs(rscDfn.getName())
                .concatWith(restoreSnap(rscName, safetySnapDfn.getName()));
        }
        else
        {
            List<String> nodeNamesNoRsc = new ArrayList<>();
            boolean updateRscDfn = false;
            for (Snapshot snap : safetySnapDfn.getAllSnapshots(peerAccCtx.get()))
            {
                NodeName nodeName = snap.getNodeName();
                @Nullable Resource stillExistingRsc = rscDfn.getResource(peerAccCtx.get(), nodeName);
                if (stillExistingRsc == null)
                {
                    nodeNamesNoRsc.add(nodeName.displayValue);
                }
                else
                {
                    StateFlags<Flags> rscFlags = stillExistingRsc.getStateFlags();
                    if (rscFlags.isSomeSet(peerAccCtx.get(), Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE))
                    {
                        rscFlags.disableFlags(peerAccCtx.get(), Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE);
                        updateRscDfn = true;
                    }
                }
            }
            Flux<ApiCallRc> restoreSafetySnapFlux = ctrlSnapRstApiCallHandler.restoreSnapshot(
                nodeNamesNoRsc,
                rscName,
                safetySnapDfn.getName(),
                rscName,
                Collections.emptyMap()
            );
            if (updateRscDfn)
            {
                flux = ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, restoreSafetySnapFlux)
                    .transform(
                        updateResponses -> CtrlResponseUtils.combineResponses(
                            errorReporter,
                            updateResponses,
                            rscName,
                            "Deactivated resource {1} on {0} for rollback"
                        )
                    )
                    .concatWith(restoreSafetySnapFlux);
            }
            else
            {
                flux = restoreSafetySnapFlux;
            }
        }
        return flux;
    }

    /**
     * Make available all resources given in the nodes collection.
     * @param rscNameStr Resource to make available
     * @param rscNodes a list with pairs of node names and indication if they should be diskful.
     * @return flux aplicallrc with results.
     */
    private Flux<ApiCallRc> recreateResources(String rscNameStr, Map<NodeName, Boolean> rscNodes)
    {
        Flux<ApiCallRc> ret = Flux.empty();
        for (Map.Entry<NodeName, Boolean> rscState : rscNodes.entrySet())
        {
            ret = ret.concatWith(ctrlRscMakeAvailableApiCallHandler.makeResourceAvailable(
                rscState.getKey().getDisplayName(),
                rscNameStr,
                Collections.emptyList(),
                rscState.getValue()
            ));
        }

        return ret;
    }

    private Flux<ApiCallRc> deleteSafetySnap(@Nullable Throwable excRef, ResourceDefinition rscDfn)
    {
        Flux<ApiCallRc> ret = Flux.empty();
        @Nullable SnapshotDefinition safetySnapDfn = findSafetySnapDfn(rscDfn);
        if (safetySnapDfn != null)
        {
            ret = ctrlSnapDelHandler.deleteSnapshot(
                rscDfn.getName(),
                safetySnapDfn.getName(),
                null
            );
            if (excRef != null)
            {
                ret = ret.concatWith(Flux.error(excRef));
            }
        }
        return ret;
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

    /**
     * Get the current nodes(objA) with resource and if the rsc is diskful(objB)
     * @param rscDfn Resource definition to check
     * @return a pair with NodeName and a boolean indicating if the resource is diskful
     */
    private Map<NodeName, Boolean> currentRscNodeDisks(ResourceDefinition rscDfn)
    {
        HashMap<NodeName, Boolean> nodes = new HashMap<>();
        Iterator<Resource> rscIter = ctrlSnapshotHelper.iterateResource(rscDfn);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            nodes.put(rsc.getNode().getName(), !isDiskless(rsc));
        }

        return nodes;
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
