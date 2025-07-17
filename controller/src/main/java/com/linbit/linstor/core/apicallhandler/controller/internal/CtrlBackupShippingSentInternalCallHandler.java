package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackgroundRunner;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRemoteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlBackupShippingAbortHandler;
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
import com.linbit.linstor.core.objects.Snapshot;
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
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.StltRemoteCleanupTask;
import com.linbit.linstor.tasks.TaskScheduleService;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlBackupShippingSentInternalCallHandler
{
    private static final int CLEANUP_AFTER = 30 * 60 * 1000;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final Provider<AccessContext> peerAccCtx;
    private final AccessContext sysCtx;
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
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @SystemContext AccessContext sysCtxRef,
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
        peerAccCtx = peerAccCtxRef;
        sysCtx = sysCtxRef;
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
        boolean successRef
    )
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Finish sending backup",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> shippingSentInTransaction(rscNameRef, snapNameRef, successRef)
            );
    }

    /**
     * Makes sure all flags and props that are needed to trigger a shipping are removed properly,
     * and start different cleanup-actions depending on the success of the shipping.
     * Also triggers the scheduled-shipping-logic if applicable to make sure the next task
     * gets started on time and all backups and snaps that go over the limit are deleted.
     * Finally starts new shipments from the queue if applicable.
     */
    private Flux<ApiCallRc> shippingSentInTransaction(String rscNameRef, String snapNameRef, boolean successRef)
        throws IOException
    {
        errorReporter.logInfo(
            "Backup shipping for snapshot %s of resource %s %s",
            snapNameRef,
            rscNameRef,
            successRef ? "finished successfully" : "failed"
        );
        SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameRef, snapNameRef, false);
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
                if (!successRef && snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING_ABORT))
                {
                    // handle abort - no flag/prop cleanup needed
                    handleResult = handleBackupAbort(snapDfn, nodeName, successRef);
                    forceSkip = true;
                }
                else
                {
                    // handle flag/prop cleanup
                    ret = ctrlRemoteApiCallHandler.cleanupRemotesIfNeeded(
                        backupInfoMgr.abortCreateDeleteEntries(nodeName.displayValue, rscNameRef, snapNameRef)
                    );
                    doStltCleanup = true;
                    handleResult = cleanupFlagsNProps(snapDfn, nodeName, successRef);
                    snapDfn.getFlags().disableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
                    if (successRef)
                    {
                        snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED);
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
                                snapNameRef
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
                    remoteForSchedule = remoteRepo.get(sysCtx, ((StltRemote) handleResult.objB).getLinstorRemoteName());
                }
                else
                {
                    remoteForSchedule = handleResult.objB;
                }
                String scheduleName = snapDfn.getSnapDfnProps(peerAccCtx.get())
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
                            peerAccCtx.get(),
                            backupInfoMgr.getL2LSrcData(remoteForSchedule.getName(), handleResult.objB.getName()),
                            backupL2LSrcApiCallHandler,
                            backgroundRunner,
                            successRef
                        );
                        taskScheduleService.rescheduleAt(cleanupTask, CLEANUP_AFTER);
                        backupInfoMgr.addTaskToCleanupData(stltRemote, cleanupTask);
                    }
                }
            }
            catch (
                AccessDeniedException | InvalidNameException | InvalidValueException | InvalidKeyException exc
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
     * @return Pair&lt;Flux, S3Remote/StltRemote&gt;
     */
    private Pair<Flux<ApiCallRc>, AbsRemote> cleanupFlagsNProps(
        SnapshotDefinition snapDfn,
        NodeName nodeName,
        boolean successRef
    ) throws InvalidKeyException, AccessDeniedException, DatabaseException, InvalidValueException, InvalidNameException
    {
        Snapshot snap = snapDfn.getSnapshot(peerAccCtx.get(), nodeName);
        AbsRemote remote;
        if (snap != null && !snap.isDeleted())
        {
            String remoteName = snap.getSnapProps(peerAccCtx.get())
                .removeProp(
                    InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                );
            snap.getFlags().disableFlags(sysCtx, Snapshot.Flags.BACKUP_SOURCE);
            AbsRemote tmpRemote = remoteRepo.get(sysCtx, new RemoteName(remoteName, true));
            // no need to update rscDfn since this only sets a prop the stlt does not care about
            // also, this method returns the linstorRemote if it got a stltRemote
            remote = getRemoteForScheduleAndCleanupFlux(
                tmpRemote,
                snapDfn.getResourceDefinition(),
                snapDfn.getName().displayValue,
                successRef
            );
        }
        else
        {
            remote = null;
        }
        return new Pair<>(Flux.empty(), remote);
    }

    /**
     * @return Pair&lt;Flux, S3Remote/StltRemote&gt;
     */
    private Pair<Flux<ApiCallRc>, AbsRemote> handleBackupAbort(
        SnapshotDefinition snapDfn,
        NodeName nodeName,
        boolean successRef
    ) throws InvalidKeyException, AccessDeniedException, DatabaseException, InvalidValueException, InvalidNameException
    {
        AbsRemote remote;
        // re-enable shipping-flag to make sure the abort-logic gets triggered later on
        snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
        ResourceDefinition rscDfn = snapDfn.getResourceDefinition();
        String snapNameStr = snapDfn.getName().displayValue;
        ctrlTransactionHelper.commit();
        Flux<ApiCallRc> flux = ctrlSnapShipAbortHandler
            .abortBackupShippingPrivileged(snapDfn, false)
            .concatWith(
                backupHelper.startStltCleanup(
                    peerProvider.get(),
                    rscDfn.getName().displayValue,
                    snapNameStr
                )
            );
        Snapshot snap = snapDfn.getSnapshot(peerAccCtx.get(), nodeName);
        if (snap != null && !snap.isDeleted())
        {
            // no idea how snap could be null or deleted here, but keep check just in case
            String remoteName = snap.getSnapProps(peerAccCtx.get())
                .removeProp(
                    InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                );
            AbsRemote tmpRemote = remoteRepo.get(sysCtx, new RemoteName(remoteName, true));
            // no need to update rscDfn since this only sets a prop the stlt does not care about
            // also, this method returns the linstorRemote if it got a stltRemote
            remote = getRemoteForScheduleAndCleanupFlux(
                tmpRemote,
                rscDfn,
                snapNameStr,
                successRef
            );
        }
        else
        {
            remote = null;
        }
        return new Pair<>(flux, remote);
    }

    private Flux<ApiCallRc> handleSchedulesIfNeeded(
        boolean successRef,
        SnapshotDefinition snapDfn,
        boolean forceSkip,
        @Nullable AbsRemote remoteForSchedule,
        NodeName nodeName,
        @Nullable String scheduleName
    ) throws AccessDeniedException, IOException
    {
        Flux<ApiCallRc> flux = Flux.empty();
        if (scheduleName != null && remoteForSchedule != null)
        {
            ResourceDefinition rscDfn = snapDfn.getResourceDefinition();
            Schedule schedule = ctrlApiDataLoader.loadSchedule(scheduleName, false);
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
        throws InvalidKeyException, AccessDeniedException, DatabaseException, InvalidValueException
    {
        AbsRemote remoteForSchedule;
        if (remote != null)
        {
            remoteForSchedule = remote;
            if (remote instanceof StltRemote)
            {
                // get the linstor-remote instead to set the prop
                AbsRemote linstorRemote = remoteRepo.get(sysCtx, ((StltRemote) remote).getLinstorRemoteName());
                if (success)
                {
                    rscDfn.getProps(peerAccCtx.get())
                        .setProp(
                            InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                            snapName,
                            ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" +
                                linstorRemote.getName().displayValue
                        );
                }
            }
            else
            {
                if (success)
                {
                    rscDfn.getProps(peerAccCtx.get())
                        .setProp(
                            InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                            snapName,
                            ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" + remote.getName().displayValue
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
