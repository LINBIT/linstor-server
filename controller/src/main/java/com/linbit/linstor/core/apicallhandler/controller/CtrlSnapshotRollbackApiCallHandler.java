package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.SharedResourceManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.mgr.SnapshotRollbackManager;
import com.linbit.linstor.core.apicallhandler.controller.utils.SnapshotRollbackChecks;
import com.linbit.linstor.core.apicallhandler.controller.utils.ZfsChecks;
import com.linbit.linstor.core.apicallhandler.controller.utils.ZfsRollbackStrategy;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.tasks.ScheduleBackupService;
import com.linbit.linstor.utils.PropsUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.StringUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

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

import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.makeSnapshotContext;
import static com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller.notConnectedError;
import static com.linbit.utils.StringUtils.firstLetterCaps;

/**
 * Rolls a resource back to a snapshot state.
 * <p>
 *      Since 1.32.0 we re-introduced the old rollback behavior. Depending on
 *      {@value ZfsRollbackStrategy#FULL_KEY_USE_ZFS_ROLLBACK_PROP}, either the old rollback is performed or the new.
 *      <ul>
 *          <li>Old behavior:
 *              <p>Only applicable for ZFS resources</p>
 *              <p>Simply set the {@value ApiConsts#KEY_RSC_ROLLBACK_TARGET} property which leads to
 *                  <code>zfs rollback ...</code> command</p>
 *          </li>
 *          <li>New behavior:
 *              <p>Forced for LVM, optional for ZFS</p>
 *              <ol>
 *                  <li>Create a SAFETY_SNAP snapshot on all nodes with a diskful resource.
 *                      <p>If this step fails, the SAFETY_SNAP is removed again.</p>
 *                  </li>
 *                  <li>All resources will be deleted (via {@link CtrlRscDfnTruncateApiCallHandler}, i.e. diskless
 *                      first)
 *                      <p>If this step fails, SAFTEY_SNAP will be restored on the diskful nodes that were
 *                          successfully deleted. Other resources are undeleted.</p>
 *                  </li>
 *                  <li>The given snapshot will be restored on the nodes that have the snapshot (which might be
 *                      different nodes then we just deleted the resources from)
 *                      <p>If this step fails, all existing resources (i.e. those which successfully performed a
 *                          resource, if any) will be deleted an all nodes will restore back to SAFETY_SNAP.</p>
 *                  </li>
 *                  <li>SAFETY_SNAP will be deleted</li>
 *              </ol>
 *          </li>
 *      </ul>
 * </p>
 */
@Singleton
public class CtrlSnapshotRollbackApiCallHandler implements CtrlSatelliteConnectionListener
{
    private static final String SAFETY_SNAP_PREFIX = "safety-snap-";
    private static final int SAFETY_SNAP_SUFFIX_ID_LEN = 10;

    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlSnapshotHelper ctrlSnapshotHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
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
    private final CtrlVlmDfnCrtApiHelper ctrlVlmDfnCrtApiHelper;
    private final ZfsChecks zfsChecks;
    private final SnapshotRollbackChecks snapshotRollbackChecks;
    private final CtrlRscCrtApiHelper ctrlRscCrtApiHelper;
    private final SharedResourceManager sharedRscMgr;

    @Inject
    public CtrlSnapshotRollbackApiCallHandler(
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSnapshotHelper ctrlSnapshotHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        BackupInfoManager backupInfoMgrRef,
        SnapshotRollbackManager snapRollbackMgrRef,
        ErrorReporter errorReporterRef,
        CtrlSnapshotCrtApiCallHandler ctrlSnapshotCrtHandlerRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDelHandlerRef,
        CtrlRscDfnTruncateApiCallHandler ctrlRscDfnTruncateApiCallHandlerRef,
        ScheduleBackupService scheduleServiceRef,
        CtrlSnapshotRestoreApiCallHandler ctrlSnapRstApiCallHandlerRef,
        CtrlSnapshotCrtHelper ctrlSnapCrtHelperRef,
        CtrlRscMakeAvailableApiCallHandler ctrlRscMakeAvailableApiCallHandlerRef,
        CtrlVlmDfnCrtApiHelper ctrlVlmDfnCrtApiHelperRef,
        ZfsChecks zfsChecksRef,
        CtrlRscCrtApiHelper ctrlRscCrtApiHelperRef,
        SharedResourceManager sharedRscMgrRef,
        SnapshotRollbackChecks snapshotRollbackChecksRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSnapshotHelper = ctrlSnapshotHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
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
        ctrlVlmDfnCrtApiHelper = ctrlVlmDfnCrtApiHelperRef;
        zfsChecks = zfsChecksRef;
        snapshotRollbackChecks = snapshotRollbackChecksRef;
        ctrlRscCrtApiHelper = ctrlRscCrtApiHelperRef;
        sharedRscMgr = sharedRscMgrRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
    {
        @Nullable String rollbackTargetSnapName = null;

        Iterator<Resource> rscIter = rscDfn.iterateResource();
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            @Nullable String snapNameStr = rsc.getProps().map().get(ApiConsts.KEY_RSC_ROLLBACK_TARGET);
            if (snapNameStr != null)
            {
                rollbackTargetSnapName = snapNameStr;
            }
        }

        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();
        if (rollbackTargetSnapName != null)
        {
            fluxes.add(updateForRollback(rscDfn.getName(), rollbackTargetSnapName));
        }

        for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns())
        {
            if (snapshotDfn.getFlags().isSet(SnapshotDefinition.Flags.SAFETY_SNAPSHOT) &&
                snapshotDfn.getFlags().isUnset(SnapshotDefinition.Flags.DELETE))
            {
                fluxes.add(recoverFailedRollback(rscDfn, snapshotDfn));
            }
        }

        return fluxes;
    }

    public Flux<ApiCallRc> rollbackSnapshot(
        String rscNameStr,
        String snapshotNameStr,
        @Nullable String zfsRollbackStrategyRef
    )
    {
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeModifyOperation(),
            Collections.emptyList(),
            rscNameStr,
            snapshotNameStr
        );

        // a shared-SP resource that is not active anywhere has to be activated first: both the
        // safety-snapshot and the rollback itself are only performed by the node using the shared data
        return ctrlSnapshotCrtHandler.activateSharedRscs(rscNameStr, Collections.emptyList())
            .concatWith(
                scopeRunner.fluxInTransactionalScope(
                    "prepare rollback",
                    lockGuardFactory.create()
                        .read(LockObj.NODES_MAP)
                        .write(LockObj.RSC_DFN_MAP)
                        .buildDeferred(),
                    () -> prepareRollbackInTransaction(rscNameStr, snapshotNameStr, zfsRollbackStrategyRef)
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> prepareRollbackInTransaction(
        String rscNameStr,
        String snapshotNameStr,
        @Nullable String zfsRollbackStrategyRef
    )
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameStr, snapshotNameStr);
        ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();

        boolean useOldRollback = snapshotRollbackChecks.useOldRollback(snapshotDfn, zfsRollbackStrategyRef);

        ResourceName rscName = rscDfn.getName();
        ensureNoBackupRestoreRunning(rscDfn);
        ensureNoScheduleActive(rscNameStr);
        ctrlSnapshotHelper.ensureSnapshotSuccessful(snapshotDfn);
        ensureAllSatellitesConnected(rscDfn);
        ensureNoResourcesInUse(rscDfn);

        Flux<ApiCallRc> retFlux;

        try
        {
            if (useOldRollback)
            {
                // old mechanic, "rollback via 'zfs rollback'". Currently only possibly in ZFS case
                zfsChecks.ensureMostRecentSnapshot(snapshotDfn);
                ensureSnapshotsForAllVolumes(snapshotDfn);
                ensureNoEbsInitiator(snapshotDfn);
                ensureSnapshotsForAllVolumes(snapshotDfn);

                markDown(rscDfn);

                ctrlTransactionHelper.commit();
                SnapshotName snapshotName = snapshotDfn.getName();
                ApiCallRc responses = ApiCallRcImpl.singletonApiCallRc(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.MODIFIED,
                        firstLetterCaps(getSnapshotDfnDescriptionInline(rscName, snapshotName)) +
                            " marked down for rollback."
                    )
                );
                Flux<ApiCallRc> nextStep = resetVlmDfns(rscDfn, snapshotDfn)
                    .concatWith(startRollback(rscName, snapshotName));
                retFlux = Flux
                    .just(responses)
                    .concatWith(
                        ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, notConnectedError(), nextStep)
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
            else
            {
                // new mechanic, "rollback via restore"
                Map<NodeName, Boolean> rscNodes = currentRscNodeDisks(rscDfn);
                List<String> sharedRestoreNodes = sharedSpActiveNodeNames(rscDfn);

                ApiCallRcImpl responses = new ApiCallRcImpl();

                SnapshotDefinition safetySnapDfn = ctrlSnapCrtHelper.createSnapshots(
                    Collections.emptyList(),
                    rscName,
                    new SnapshotName(SAFETY_SNAP_PREFIX + StringUtils.randomAlphaNumString(SAFETY_SNAP_SUFFIX_ID_LEN)),
                    Collections.emptyMap(),
                    responses
                );
                safetySnapDfn.getFlags().enableFlags(SnapshotDefinition.Flags.SAFETY_SNAPSHOT);
                ctrlTransactionHelper.commit();
                retFlux = ctrlSnapshotCrtHandler.postCreateSnapshot(safetySnapDfn, false)
                    .concatWith(Flux.<ApiCallRc>just(responses))
                    .concatWith(
                        deleteRscs(rscDfn.getName())
                            .onErrorResume(
                                exc -> rollbackToSafetySnap(snapshotDfn, false).concatWith(Flux.error(exc))
                            )
                    )
                    .concatWith(
                        restoreSnap(rscDfn, snapshotDfn, sharedRestoreNodes)
                            .onErrorResume(
                                exc -> rollbackToSafetySnap(snapshotDfn, true).concatWith(Flux.error(exc))
                            )
                    )
                    .concatWith(deleteSafetySnap(null, rscDfn))
                    .concatWith(recreateResources(rscNameStr, rscNodes))
                    .onErrorResume(exc -> deleteSafetySnap(exc, rscDfn));
            }
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        return retFlux;
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

    /**
     * Start transactional scope for {@link #resetVlmDfnsInTransaction(ResourceDefinition, SnapshotDefinition)}
     *
     *
     */
    private Flux<ApiCallRc> resetVlmDfns(ResourceDefinition targetRscDfn, SnapshotDefinition srcSnapDfn)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "prepare rollback - reset vlmDfns",
                lockGuardFactory.create()
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> resetVlmDfnsInTransaction(targetRscDfn, srcSnapDfn)
            );
    }

    /**
     * Ensure the volume definitions are in the same state they were in when the snapshot was made.
     * This includes deleting vlmDfns that were created after that snapshot, and recreating vlmDfns that had since been
     * deleted, as well as ensuring the sizes and properties of all vlmDfns are those from the snapshot.
     *
     *
     *
     */
    private Flux<ApiCallRc> resetVlmDfnsInTransaction(ResourceDefinition targetRscDfn, SnapshotDefinition srcSnapDfn)
        throws DatabaseException, ImplementationError
    {
        // since vlmNrs can be chosen arbitrarily by the user, first remove any vlmDfns that did not exist when the
        // snapshot was created
        Iterator<VolumeDefinition> rscVlmDfnIterator = targetRscDfn.iterateVolumeDfn();
        ArrayList<VolumeDefinition> vlmDfnsToDelete = new ArrayList<>();
        while (rscVlmDfnIterator.hasNext())
        {
            VolumeDefinition rscVlmDfn = rscVlmDfnIterator.next();
            if (srcSnapDfn.getSnapshotVolumeDefinition(rscVlmDfn.getVolumeNumber()) == null)
            {
                vlmDfnsToDelete.add(rscVlmDfn);
            }
        }
        for (VolumeDefinition vlmDfnToDelete : vlmDfnsToDelete)
        {
            vlmDfnToDelete.delete();
        }
        // now create any vlmDfns that have been deleted since the snap was made, and modify all props and sizes of
        // still existing vlmDfns
        for (SnapshotVolumeDefinition snapVlmDfn : srcSnapDfn.getAllSnapshotVolumeDefinitions())
        {
            @Nullable VolumeDefinition targetVlmDfn = targetRscDfn.getVolumeDfn(
                snapVlmDfn.getVolumeNumber()
            );
            long snapVlmSize = snapVlmDfn.getVolumeSize();
            if (targetVlmDfn == null)
            {
                targetVlmDfn = ctrlVlmDfnCrtApiHelper.createVlmDfnData(
                    targetRscDfn,
                    snapVlmDfn.getVolumeNumber(),
                    null,
                    snapVlmSize,
                    VolumeDefinition.Flags.restoreFlags(0)
                );
            }
            else
            {
                try
                {
                    targetVlmDfn.setVolumeSize(snapVlmSize);
                }
                catch (MinSizeException | MaxSizeException exc)
                {
                    throw new ImplementationError("Invalid size during snapshot rollback", exc);
                }
            }
            ReadOnlyProps vlmDfnProps = snapVlmDfn.getVlmDfnProps();
            PropsUtils.resetProps(vlmDfnProps.map(), targetVlmDfn.getProps());
        }
        ctrlTransactionHelper.commit();
        return ctrlSatelliteUpdateCaller.updateSatellites(
            targetRscDfn,
            nodeName -> Flux.error(new ApiRcException(ResponseUtils.makeNotConnectedWarning(nodeName))),
            Flux.empty()
        )
            .transform(
                updateResponses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    updateResponses,
                    targetRscDfn.getName(),
                    "Rsc {1} on {0} updated"
                )
            );
    }

    private Flux<ApiCallRc> restoreSnap(ResourceDefinition rscDfn, SnapshotDefinition snapDfn)
    {
        return restoreSnap(rscDfn, snapDfn, Collections.emptyList());
    }

    /**
     * @param nodeNamesRef the nodes to restore on; empty means all nodes holding the snapshot (of
     *     which the restore handler picks a single one for shared storage pools). The rollback of a
     *     shared-SP resource passes the nodes that were active before the resources were deleted, so
     *     the restored (active) resource ends up where the rolled-back one was used.
     */
    private Flux<ApiCallRc> restoreSnap(
        ResourceDefinition rscDfn,
        SnapshotDefinition snapDfn,
        List<String> nodeNamesRef
    )
    {
        ResourceName rscName = rscDfn.getName();
        // ensure the vlmDfns are in the state that they were when the snapshot was made
        return resetVlmDfns(rscDfn, snapDfn)
            .concatWith(
                ctrlSnapRstApiCallHandler.restoreSnapshotForRollback(
                    nodeNamesRef,
                    rscName,
                    snapDfn.getName(),
                    rscName,
                    Collections.emptyMap()
                )
            );
    }

    /**
     * Names of the nodes holding an active copy of a shared-SP resource of the given
     * resource-definition. Empty for resource-definitions without shared storage pools.
     */
    private List<String> sharedSpActiveNodeNames(ResourceDefinition rscDfn)
    {
        List<String> ret = new ArrayList<>();
        Iterator<Resource> rscIter = ctrlSnapshotHelper.iterateResource(rscDfn);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (sharedRscMgr.isBackedBySharedStorPool(rsc) &&
                !rsc.getStateFlags().isSomeSet(
                    Resource.Flags.INACTIVE,
                    Resource.Flags.INACTIVE_PERMANENTLY
                ))
            {
                ret.add(rsc.getNode().getName().displayValue);
            }
        }
        return ret;
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
        for (var snapDfn : rscDfn.getSnapshotDfns())
        {
            if (snapDfn.getFlags().isSet(SnapshotDefinition.Flags.SAFETY_SNAPSHOT))
            {
                snapDfnRet = snapDfn;
                break;
            }
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
     */
    private Flux<ApiCallRc> rollbackToSafetySnapInTransaction(SnapshotDefinition snapDfn, boolean restoreStarted)
        throws DatabaseException
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
                .concatWith(restoreSnap(rscDfn, safetySnapDfn));
        }
        else
        {
            List<String> nodeNamesNoRsc = new ArrayList<>();
            boolean updateRscDfn = false;
            for (Snapshot snap : safetySnapDfn.getAllSnapshots())
            {
                NodeName nodeName = snap.getNodeName();
                @Nullable Resource stillExistingRsc = rscDfn.getResource(nodeName);
                if (stillExistingRsc == null)
                {
                    nodeNamesNoRsc.add(nodeName.displayValue);
                }
                else
                {
                    StateFlags<Flags> rscFlags = stillExistingRsc.getStateFlags();
                    if (rscFlags.isSomeSet(Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE))
                    {
                        rscFlags.disableFlags(Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE);
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
                rscState.getValue(),
                null,
                false,
                Collections.emptyList()
            ));
        }

        return ret;
    }

    private Flux<ApiCallRc> deleteSafetySnap(@Nullable Throwable excRef, ResourceDefinition rscDfn)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "delete safety-snap",
                lockGuardFactory.create()
                    .read(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> deleteSafetySnapInScope(excRef, rscDfn)
            );
    }

    private Flux<ApiCallRc> deleteSafetySnapInScope(@Nullable Throwable excRef, ResourceDefinition rscDfn)
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
    private Flux<ApiCallRc> updateForRollback(ResourceName rscName, String snapNameStr)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Update for rollback",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                () -> updateForRollbackInScope(rscName, snapNameStr)
            );
    }

    private Flux<ApiCallRc> updateForRollbackInScope(ResourceName rscName, String snapNameStr)
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName);

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

        Flux<ApiCallRc> finishRollback = finishRollback(rscName, snapNameStr);
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
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName);

        getProps(rsc).map().remove(ApiConsts.KEY_RSC_ROLLBACK_TARGET);

        ctrlTransactionHelper.commit();

        return Flux.empty();
    }

    private Flux<ApiCallRc> finishRollback(ResourceName rscName, String snapNameStr)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Reactivate resources after rollback",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                () -> finishRollbackInScope(rscName, snapNameStr)
            );
    }

    private Flux<ApiCallRc> finishRollbackInScope(ResourceName rscName, String snapNameStr)
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName);
        unmarkDownPrivileged(rscDfn);

        ctrlTransactionHelper.commit();

        Set<Resource> diskRscSet = new HashSet<>();
        Iterator<Resource> rscIter = iterateResourcePrivileged(rscDfn);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (!isDisklessPrivileged(rsc))
            {
                diskRscSet.add(rsc);
            }
        }
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeModifyOperation(),
            Collections.emptyList(),
            rscName.displayValue,
            snapNameStr
        );

        return ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
            .transform(responses -> CtrlResponseUtils.combineResponses(
                errorReporter,
                responses,
                rscName,
                "Re-activated resource {1} on {0} after rollback"
            ))
            // do not report the rollback as finished before the re-activated resources are
            // actually ready again (same semantics as resource create / snapshot restore)
            .concatWith(ctrlRscCrtApiHelper.waitResourcesReady(context, rscDfn, diskRscSet));
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
            // an inactive shared-SP copy does not participate in the rollback and is not
            // recreated afterwards: making it available again would move the activation to a
            // node without the snapshots, which is refused
            if (!sharedRscMgr.isInactiveShared(rsc))
            {
                nodes.put(rsc.getNode().getName(), !isDiskless(rsc));
            }
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
        return rsc.isDrbdDiskless() || rsc.isNvmeInitiator() || rsc.isEbsInitiator();
    }

    private Optional<Resource> anyResourceInUse(ResourceDefinition rscDfn)
    {
        Optional<Resource> rscInUse;
        rscInUse = rscDfn.anyResourceInUse();
        return rscInUse;
    }

    private boolean isDisklessPrivileged(Resource rsc)
    {
        boolean diskless;
        diskless = rsc.isDrbdDiskless() || rsc.isNvmeInitiator();
        return diskless;
    }

    private void unmarkDownPrivileged(ResourceDefinition rscDfn)
    {
        try
        {
            Map<String, DrbdRscDfnData<Resource>> drbdRscDfnDataMap = rscDfn.getLayerData(
                DeviceLayerKind.DRBD
            );
            for (DrbdRscDfnData<Resource> drbdRscDfnData : drbdRscDfnDataMap.values())
            {
                drbdRscDfnData.setDown(false);
            }
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private Iterator<Resource> iterateResourcePrivileged(ResourceDefinition rscDfn)
    {
        Iterator<Resource> rscIter;
        rscIter = rscDfn.iterateResource();
        return rscIter;
    }

    private Props getProps(Resource rsc)
    {
        Props props;
        props = rsc.getProps();
        return props;
    }

    /*
     * ***********
     * Methods for old snapshot rollback behavior
     * ***********
     */

    private boolean allVolumesHaveSnapshots(SnapshotDefinition snapDfnRef)
    {
        boolean ret = true;
        Iterator<Resource> rscIter = ctrlSnapshotHelper.iterateResource(snapDfnRef.getResourceDefinition());
        while (ret && rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (!isDiskless(rsc))
            {
                Snapshot snapshot = ctrlApiDataLoader.loadSnapshot(rsc.getNode(), snapDfnRef);
                Iterator<Volume> vlmIter = rsc.iterateVolumes();
                while (ret && vlmIter.hasNext())
                {
                    Volume vlm = vlmIter.next();
                    @Nullable SnapshotVolume snapVlm = snapshot.getVolume(vlm.getVolumeNumber());
                    if (snapVlm == null)
                    {
                        ret = false;
                    }
                }
            }
        }
        return ret;
    }

    private void ensureNoEbsInitiator(SnapshotDefinition snapDfnRef)
    {
        Iterator<Resource> rscIter = ctrlSnapshotHelper.iterateResource(snapDfnRef.getResourceDefinition());
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (isEbsInitiator(rsc))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_IN_USE,
                        "Cannot rollback EBS volume while attached."
                    )
                        .setCorrection("Delete the EBS initiator resource(s) first")
                        .setSkipErrorReport(true)
                );
            }
        }
    }

    private void ensureSnapshotsForAllVolumes(SnapshotDefinition snapDfnRef)
    {
        if (!allVolumesHaveSnapshots(snapDfnRef))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT,
                    "Some volumes were not captured by the snapshot. Cannot rollback."
                )
                    .setSkipErrorReport(true)
            );
        }
    }

    private boolean isEbsInitiator(Resource rsc)
    {
        return rsc.isEbsInitiator();
    }

    private void markDown(ResourceDefinition rscDfn)
    {
        try
        {
            Map<String, DrbdRscDfnData<Resource>> drbdRscDfnDataMap = rscDfn.getLayerData(
                DeviceLayerKind.DRBD
            );
            for (DrbdRscDfnData<Resource> drbdRscDfnData : drbdRscDfnDataMap.values())
            {
                drbdRscDfnData.setDown(true);
            }
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
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
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName);
        unmarkDownPrivileged(rscDfn);
        ctrlTransactionHelper.commit();
        Flux<ApiCallRc> nextStep = Flux.error(exception);
        return ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, nextStep)
            // ensure that the individual node update fluxes are subscribed to, but ignore the responses
            .flatMap(Tuple2::getT2)
            .thenMany(Flux.<ApiCallRc>empty())
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
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName);
        ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();
        Iterator<Resource> rscIter = iterateResourcePrivileged(rscDfn);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            getProps(rsc).map().put(ApiConsts.KEY_RSC_ROLLBACK_TARGET, snapshotName.displayValue);
        }
        ctrlTransactionHelper.commit();
        return updateForRollback(rscName, snapshotName.displayValue);
    }
}
