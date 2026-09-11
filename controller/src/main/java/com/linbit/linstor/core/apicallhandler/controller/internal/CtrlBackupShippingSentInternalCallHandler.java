package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.BackgroundRunner;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlBackupShippingAbortHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRemoteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlScheduledBackupsApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Schedule;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.tasks.StltRemoteCleanupTask;
import com.linbit.linstor.tasks.TaskScheduleService;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.Pair;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.io.IOException;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlBackupShippingSentInternalCallHandler
{
    private static final int CLEANUP_AFTER = 30 * 60 * 1000;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ErrorReporter errorReporter;
    private final Provider<Peer> peerProvider;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlBackupShippingAbortHandler ctrlSnapShipAbortHandler;
    private final RemoteRepository remoteRepo;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlBackupApiHelper backupHelper;
    private final CtrlScheduledBackupsApiCallHandler scheduledBackupsHandler;
    private final CtrlBackupQueueInternalCallHandler queueHandler;
    private final TaskScheduleService taskScheduleService;
    private final CtrlBackupL2LSrcApiCallHandler backupL2LSrcApiCallHandler;
    private final BackgroundRunner backgroundRunner;
    private final CtrlRemoteApiCallHandler ctrlRemoteApiCallHandler;

    @Inject
    public CtrlBackupShippingSentInternalCallHandler(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ErrorReporter errorReporterRef,
        Provider<Peer> peerProviderRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlBackupShippingAbortHandler ctrlSnapShipAbortHandlerRef,
        RemoteRepository remoteRepoRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlBackupApiHelper backupHelperRef,
        CtrlScheduledBackupsApiCallHandler scheduledBackupsHandlerRef,
        CtrlBackupQueueInternalCallHandler queueHandlerRef,
        TaskScheduleService taskScheduleServiceRef,
        CtrlBackupL2LSrcApiCallHandler backupL2LSrcApiCallHandlerRef,
        BackgroundRunner backgroundRunnerRef,
        CtrlRemoteApiCallHandler ctrlRemoteApiCallHandlerRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        errorReporter = errorReporterRef;
        peerProvider = peerProviderRef;
        backupInfoMgr = backupInfoMgrRef;
        ctrlSnapShipAbortHandler = ctrlSnapShipAbortHandlerRef;
        remoteRepo = remoteRepoRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        backupHelper = backupHelperRef;
        scheduledBackupsHandler = scheduledBackupsHandlerRef;
        queueHandler = queueHandlerRef;
        taskScheduleService = taskScheduleServiceRef;
        backupL2LSrcApiCallHandler = backupL2LSrcApiCallHandlerRef;
        backgroundRunner = backgroundRunnerRef;
        ctrlRemoteApiCallHandler = ctrlRemoteApiCallHandlerRef;
    }

    /**
     * Called by the stlt as soon as it finishes shipping the backup
     */
    public Flux<ApiCallRc> shippingSent(
        String rscNameRef,
        String snapNameRef,
        boolean successRef,
        String remoteName
    )
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Finish sending backup",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> shippingSentInTransaction(rscNameRef, snapNameRef, successRef, remoteName)
            );
    }

    /**
     * Makes sure all flags and props that are needed to trigger a shipping are removed properly,
     * and start different cleanup-actions depending on the success of the shipping.
     * Also triggers the scheduled-shipping-logic if applicable to make sure the next task
     * gets started on time and all backups and snaps that go over the limit are deleted.
     * Finally starts new shipments from the queue if applicable.
     */
    private Flux<ApiCallRc> shippingSentInTransaction(
        String rscNameRef,
        String snapNameRef,
        boolean successRef,
        String remoteName
    )
        throws IOException
    {
        errorReporter.logInfo(
            "Backup shipping for snapshot %s of resource %s %s",
            snapNameRef,
            rscNameRef,
            successRef ? "finished successfully" : "failed"
        );
        @Nullable SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfnOrNull(rscNameRef, snapNameRef);
        Flux<ApiCallRc> ret = Flux.empty();
        if (snapDfn != null)
        {
            try
            {
                NodeName nodeName = peerProvider.get().getNode().getName();
                // Pair<Flux, S3Remote/StltRemote>
                Pair<Flux<ApiCallRc>, AbsRemote> handleResult;
                boolean forceSkip = false;
                boolean doStltCleanup = false;
                Props snapDfnProps = snapDfn.getSnapDfnProps();
                String propsNamespc = BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + remoteName;
                if (
                    !successRef && (
                        BackupShippingUtils.hasShippingStatus(
                            snapDfn,
                            remoteName,
                            InternalApiConsts.VALUE_ABORTING
                        ) ||
                        // A source-initiated abort might already have been requested but not yet have set
                        // VALUE_ABORTING (that happens in a separate, later transaction of the abort). It does
                        // however set the passive VALUE_PREPARE_ABORT heads-up in its first transaction. Since
                        // 'remoteName' is non-null here, both checks look at the source namespace
                        // (Source/<remote>/ShippingStatus), and VALUE_PREPARE_ABORT is only ever written into that
                        // namespace by exactly that source-abort path - so reaching this branch always means an
                        // abort really is in progress for this shipment.
                        // Yes, it looks odd to treat an "abort prepare" as if it were an actual "abort": but the
                        // shipment produces only a single shipping-sent notification, and in the racing case it
                        // arrives before VALUE_ABORTING is set. If we did not handle it as an abort here, we would
                        // miss it entirely - treating it as a normal failure, rescheduling it and leaving the
                        // snapshot behind.
                        BackupShippingUtils.hasShippingStatus(
                            snapDfn,
                            remoteName,
                            InternalApiConsts.VALUE_PREPARE_ABORT
                        )
                    )
                )
                {
                    // handle abort - no flag/prop cleanup needed
                    handleResult = handleBackupAbort(snapDfn, remoteName, successRef);
                    forceSkip = true;
                }
                else
                {
                    // handle flag/prop cleanup
                    ret = ctrlRemoteApiCallHandler.cleanupRemotesIfNeeded(
                        backupInfoMgr.abortCreateDeleteEntries(nodeName.displayValue, rscNameRef, snapNameRef)
                    );
                    doStltCleanup = true;
                    handleResult = cleanupFlagsNProps(snapDfn, successRef, remoteName);
                    if (successRef)
                    {
                        snapDfnProps.setProp(
                            InternalApiConsts.KEY_SHIPPING_STATUS,
                            InternalApiConsts.VALUE_SUCCESS,
                            propsNamespc
                        );
                    }
                    else
                    {
                        snapDfnProps.setProp(
                            InternalApiConsts.KEY_SHIPPING_STATUS,
                            InternalApiConsts.VALUE_FAILED,
                            propsNamespc
                        );
                    }
                }
                ctrlTransactionHelper.commit();
                boolean doStltCleanupCopyForEffectivelyFinal = doStltCleanup;
                ret = ret.concatWith(
                    ctrlSatelliteUpdateCaller.updateSatellites(
                        snapDfn,
                        CtrlSatelliteUpdateCaller.notConnectedWarn()
                    ).transform(
                        responses -> CtrlResponseUtils.combineResponses(
                            errorReporter,
                            responses,
                            LinstorParsingUtils.asRscName(rscNameRef),
                            "Finishing shipping of backup ''" + snapNameRef + "'' of {1} on {0}"
                        ).concatWith(
                            doStltCleanupCopyForEffectivelyFinal ? backupHelper.startStltCleanup(
                                peerProvider.get(),
                                rscNameRef,
                                snapNameRef,
                                remoteName
                            ) : Flux.empty()
                        )
                    )
                );
                // The handleResult-flux will not be executed if ret has an error - this issue is currently
                // unavoidable.
                // This will be fixed with the linstor2 issue 19 (Combine Changed* proto messages for atomic
                // updates)
                ret = ret.concatWith(handleResult.objA);
                AbsRemote remoteForSchedule;
                if (handleResult.objB instanceof StltRemote)
                {
                    remoteForSchedule = remoteRepo.get(((StltRemote) handleResult.objB).getLinstorRemoteName());
                }
                else
                {
                    remoteForSchedule = handleResult.objB;
                }
                String scheduleName = snapDfn.getSnapDfnProps()
                    .getProp(InternalApiConsts.KEY_BACKUP_SHIPPED_BY_SCHEDULE, InternalApiConsts.NAMESPC_SCHEDULE);
                // if scheduleName == null the backup did not originate from a scheduled shipping
                ret = ret.concatWith(
                    handleSchedulesIfNeeded(
                        successRef,
                        snapDfn,
                        forceSkip,
                        remoteForSchedule,
                        nodeName,
                        scheduleName
                    )
                );

                if (!successRef)
                {
                    // if an error occurred, we can already stop waiting - otherwise we need to wait for the
                    // target-cluster to be done as well
                    ret = ret.concatWith(backupInfoMgr.errorWaitForShipSentDoneFlux(snapDfn, remoteName));
                }

                if (remoteForSchedule instanceof S3Remote)
                {
                    // s3 case does not have and need a stltRemote
                    ret = ret.concatWith(queueHandler.handleBackupQueues(snapDfn, remoteForSchedule, null));
                }
                else if (remoteForSchedule instanceof LinstorRemote)
                {
                    StltRemote stltRemote = (StltRemote) handleResult.objB;
                    Flux<ApiCallRc> queueFlux = backupL2LSrcApiCallHandler.startQueueIfReady(
                        stltRemote,
                        successRef,
                        true
                    );
                    if (queueFlux != null)
                    {
                        ret = ret.concatWith(queueFlux);
                    }
                    else
                    {
                        StltRemoteCleanupTask cleanupTask = new StltRemoteCleanupTask(
                            backupInfoMgr.getL2LSrcData(remoteForSchedule.getName(), handleResult.objB.getName()),
                            backupL2LSrcApiCallHandler,
                            backgroundRunner,
                            successRef
                        );
                        taskScheduleService.rescheduleIn(cleanupTask, CLEANUP_AFTER);
                        backupInfoMgr.addTaskToCleanupData(stltRemote, cleanupTask);
                    }
                }
            }
            catch (
                InvalidNameException | InvalidValueException | InvalidKeyException exc
            )
            {
                throw new ImplementationError(exc);
            }
            catch (DatabaseException exc)
            {
                throw new ApiDatabaseException(exc);
            }
        }
        return ret;
    }

    /**
     * @param remoteNameRef
     *     - name of either an S3Remote or LinstorRemote
     *
     * @return Pair&lt;Flux, S3Remote/StltRemote&gt;
     */
    private Pair<Flux<ApiCallRc>, AbsRemote> cleanupFlagsNProps(
        SnapshotDefinition snapDfn,
        boolean successRef,
        String remoteNameRef
    ) throws InvalidKeyException, DatabaseException, InvalidValueException, InvalidNameException
    {
        AbsRemote remote;
        // get the target-remote-name - either of a S3Remote or StltRemote
        String remoteName = snapDfn.getSnapDfnProps()
            .getProp(
                InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + remoteNameRef
            );
        AbsRemote tmpRemote = remoteRepo.get(new RemoteName(remoteName, true));
        // no need to update rscDfn since this only sets a prop the stlt does not care about
        // also, this method returns the linstorRemote if it got a stltRemote
        remote = getRemoteForScheduleAndCleanupFlux(
            tmpRemote,
            snapDfn.getResourceDefinition(),
            snapDfn.getName().displayValue,
            successRef
        );
        return new Pair<>(Flux.empty(), remote);
    }

    /**
     * @param remoteNameStr
     *     - name of either an S3Remote or LinstorRemote
     *
     * @return Pair&lt;Flux, S3Remote/StltRemote&gt;
     */
    private Pair<Flux<ApiCallRc>, AbsRemote> handleBackupAbort(
        SnapshotDefinition snapDfn,
        String remoteNameStr,
        boolean successRef
    ) throws InvalidKeyException, DatabaseException, InvalidValueException, InvalidNameException
    {
        AbsRemote remote;
        ResourceDefinition rscDfn = snapDfn.getResourceDefinition();
        String snapNameStr = snapDfn.getName().displayValue;
        ctrlTransactionHelper.commit();
        // get the target-remote-name - either of a S3Remote or StltRemote
        String remoteName = snapDfn.getSnapDfnProps()
            .getProp(
                InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + remoteNameStr
            );
        AbsRemote tmpRemote = remoteRepo.get(new RemoteName(remoteName, true));
        Flux<ApiCallRc> flux = ctrlSnapShipAbortHandler
            .abortBackupShippingPrivileged(snapDfn, tmpRemote instanceof S3Remote)
            .concatWith(
                backupHelper.startStltCleanup(
                    peerProvider.get(),
                    rscDfn.getName().displayValue,
                    snapNameStr,
                    remoteNameStr
                )
            );
        // no need to update rscDfn since this only sets a prop the stlt does not care about
        // also, this method returns the linstorRemote if it got a stltRemote
        remote = getRemoteForScheduleAndCleanupFlux(
            tmpRemote,
            rscDfn,
            snapNameStr,
            successRef
        );
        return new Pair<>(flux, remote);
    }

    private Flux<ApiCallRc> handleSchedulesIfNeeded(
        boolean successRef,
        SnapshotDefinition snapDfn,
        boolean forceSkip,
        @Nullable AbsRemote remoteForSchedule,
        NodeName nodeName,
        @Nullable String scheduleName
    ) throws IOException
    {
        Flux<ApiCallRc> flux = Flux.empty();
        if (scheduleName != null && remoteForSchedule != null)
        {
            ResourceDefinition rscDfn = snapDfn.getResourceDefinition();
            @Nullable Schedule schedule = ctrlApiDataLoader.loadScheduleOrNull(scheduleName);
            if (schedule != null)
            {
                boolean lastBackupIncremental = scheduledBackupsHandler.rescheduleShipping(
                    snapDfn,
                    nodeName,
                    rscDfn,
                    schedule,
                    remoteForSchedule,
                    successRef,
                    forceSkip
                );

                // delete snaps & backups if needed (only check if last backup was full
                if (!lastBackupIncremental)
                {
                    flux = scheduledBackupsHandler.checkScheduleKeep(rscDfn, schedule, remoteForSchedule);
                }
            }
            else
            {
                errorReporter.logWarning(
                    "Could not reschedule resource definition %s as schedule %s was not found",
                    rscDfn.getName().displayValue,
                    scheduleName
                );
            }
        }
        return flux;
    }

    /**
     * Returns the remote (s3 or stlt) and sets necessary props dependent on which kind of remote it is.
     */
    private AbsRemote getRemoteForScheduleAndCleanupFlux(
        AbsRemote remote,
        ResourceDefinition rscDfn,
        String snapName,
        boolean success
    )
        throws InvalidKeyException, DatabaseException, InvalidValueException
    {
        AbsRemote remoteForSchedule;
        if (remote != null)
        {
            remoteForSchedule = remote;
            if (remote instanceof StltRemote stltRemote)
            {
                // get the linstor-remote instead to set the prop
                AbsRemote linstorRemote = remoteRepo.get(stltRemote.getLinstorRemoteName());
                if (success)
                {
                    rscDfn.getProps()
                        .setProp(
                            InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                            snapName,
                            BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" +
                                linstorRemote.getName().displayValue
                        );
                }
            }
            else
            {
                if (success)
                {
                    rscDfn.getProps()
                        .setProp(
                            InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                            snapName,
                            BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + remote.getName().displayValue
                        );
                }
            }
        }
        else
        {
            throw new ImplementationError("Unknown remote. successRef: " + success);
        }
        return remoteForSchedule;
    }
}
