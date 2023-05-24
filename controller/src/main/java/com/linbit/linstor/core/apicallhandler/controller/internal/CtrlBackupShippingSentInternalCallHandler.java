package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.BackupInfoManager.QueueItem;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotShippingAbortHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupCreateApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler.BackupShippingRestClient;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlScheduledBackupsApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingRequestPrevSnap;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingResponsePrevSnap;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Node;
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
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.ExceptionThrowingIterator;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlBackupShippingSentInternalCallHandler
{
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final Provider<AccessContext> peerAccCtx;
    private final AccessContext sysCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ErrorReporter errorReporter;
    private final Provider<Peer> peerProvider;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlSnapshotShippingAbortHandler ctrlSnapShipAbortHandler;
    private final RemoteRepository remoteRepo;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlBackupApiHelper backupHelper;
    private final CtrlScheduledBackupsApiCallHandler scheduledBackupsHandler;
    private final BackupShippingRestClient restClient;
    private final Provider<CtrlBackupL2LSrcApiCallHandler> backupL2LSrcHandler;
    private final CtrlBackupCreateApiCallHandler backupCrtHandler;

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
        CtrlSnapshotShippingAbortHandler ctrlSnapShipAbortHandlerRef,
        RemoteRepository remoteRepoRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlBackupApiHelper backupHelperRef,
        CtrlScheduledBackupsApiCallHandler scheduledBackupsHandlerRef,
        BackupShippingRestClient restClientRef,
        Provider<CtrlBackupL2LSrcApiCallHandler> backupL2LSrcHandlerRef,
        CtrlBackupCreateApiCallHandler backupCrtHandlerRef
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
        restClient = restClientRef;
        backupL2LSrcHandler = backupL2LSrcHandlerRef;
        backupCrtHandler = backupCrtHandlerRef;

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
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
        boolean forceSkip = false;
        AbsRemote remote = null;
        AbsRemote remoteForSchedule = null;
        Flux<ApiCallRc> cleanupFlux = Flux.empty();
        if (snapDfn != null)
        {
            try
            {
                NodeName nodeName = peerProvider.get().getNode().getName();
                boolean doStltCleanup = false;
                if (!successRef && snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING_ABORT))
                {
                    // re-enable shipping-flag to make sure the abort-logic gets triggered later on
                    snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
                    ctrlTransactionHelper.commit();
                    cleanupFlux = ctrlSnapShipAbortHandler
                        .abortBackupShippingPrivileged(snapDfn, false)
                        .concatWith(
                            backupHelper.startStltCleanup(
                                peerProvider.get(),
                                rscNameRef,
                                snapNameRef
                            )
                        );
                    Snapshot snap = snapDfn.getSnapshot(peerAccCtx.get(), nodeName);
                    if (snap != null && !snap.isDeleted())
                    {
                        // no idea how snap could be null or deleted here, but keep check just in case
                        String remoteName = snap.getProps(peerAccCtx.get())
                            .removeProp(
                                InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                                ApiConsts.NAMESPC_BACKUP_SHIPPING
                            );
                        remote = remoteRepo.get(sysCtx, new RemoteName(remoteName, true));
                        // no need to update rscDfn since this only sets a prop the stlt does not care about
                        Pair<Flux<ApiCallRc>, AbsRemote> pair = getRemoteForScheduleAndCleanupFlux(
                            remote,
                            rscDfn,
                            snapNameRef,
                            successRef
                        );
                        remoteForSchedule = pair.objB;
                        cleanupFlux = cleanupFlux.concatWith(pair.objA);
                    }
                    forceSkip = true;
                }
                else
                {
                    backupInfoMgr.abortCreateDeleteEntries(nodeName.displayValue, rscNameRef, snapNameRef);
                    doStltCleanup = true;
                    snapDfn.getFlags().disableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
                    if (successRef)
                    {
                        snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED);
                    }
                    Snapshot snap = snapDfn.getSnapshot(peerAccCtx.get(), nodeName);
                    if (snap != null && !snap.isDeleted())
                    {
                        String remoteName = snap.getProps(peerAccCtx.get())
                            .removeProp(
                                InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                                ApiConsts.NAMESPC_BACKUP_SHIPPING
                            );
                        snap.getFlags().disableFlags(sysCtx, Snapshot.Flags.BACKUP_SOURCE);
                        remote = remoteRepo.get(sysCtx, new RemoteName(remoteName, true));
                        // no need to update rscDfn since this only sets a prop the stlt does not care about
                        Pair<Flux<ApiCallRc>, AbsRemote> pair = getRemoteForScheduleAndCleanupFlux(
                            remote,
                            rscDfn,
                            snapNameRef,
                            successRef
                        );
                        remoteForSchedule = pair.objB;
                        cleanupFlux = cleanupFlux.concatWith(pair.objA);
                    }
                }
                ctrlTransactionHelper.commit();
                Flux<ApiCallRc> flux = Flux.empty();
                boolean doStltCleanupCopyForEffectivelyFinal = doStltCleanup;
                flux = flux.concatWith(
                    ctrlSatelliteUpdateCaller.updateSatellites(
                        snapDfn,
                        CtrlSatelliteUpdateCaller.notConnectedWarn()
                    )
                        .transform(
                            responses -> CtrlResponseUtils.combineResponses(
                                responses,
                                LinstorParsingUtils.asRscName(rscNameRef),
                                "Finishing shipping of backup ''" + snapNameRef + "'' of {1} on {0}"
                            )
                                .concatWith(
                                    doStltCleanupCopyForEffectivelyFinal ? backupHelper.startStltCleanup(
                                        peerProvider.get(),
                                        rscNameRef,
                                        snapNameRef
                                    ) : Flux.empty()
                                )
                        )
                );
                // cleanupFlux will not be executed if flux has an error - this issue is currently unavoidable.
                // This will be fixed with the linstor2 issue 19 (Combine Changed* proto messages for atomic
                // updates)
                cleanupFlux = flux.concatWith(cleanupFlux);

                String scheduleName = snapDfn.getProps(peerAccCtx.get())
                    .getProp(InternalApiConsts.KEY_BACKUP_SHIPPED_BY_SCHEDULE, InternalApiConsts.NAMESPC_SCHEDULE);
                // if scheduleName == null the backup did not originate from a scheduled shipping
                if (scheduleName != null && remoteForSchedule != null)
                {
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
                            cleanupFlux = cleanupFlux.concatWith(
                                scheduledBackupsHandler.checkScheduleKeep(rscDfn, schedule, remoteForSchedule)
                            );
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
                /*
                 * Only after a node finishes a shipment will other backups queued on that node get a chance to get
                 * started.
                 * This does not necessarily have to happen in order, in case the oldest queued backup cannot be started
                 * right now, the next entry in the queue will be chosen instead.
                 * Since this is the only spot where new shipments can be started, a problem arises when it comes to
                 * incremental backups. Unless the previous backup has already finished shipping, an incremental backup
                 * always needs to be queued. This might lead to non-empty queues on nodes that are currently not
                 * shipping anything. As elaborated before, those queued backups would never be able to start on those
                 * nodes since there is no shipment that could trigger the queue.
                 * Therefore, getFollowUpSnaps is used to figure out if the backup that just finished shipping has an
                 * incremental backup queued on any node, and if so, to start it, giving those nodes a chance to start a
                 * shipment as well. Additionally this leads to a chance at filling newly added shipping slots up
                 * sooner with the incremental backups getFollowUpSnaps provides.
                 */
                Map<QueueItem, TreeSet<Node>> followUpSnaps = backupInfoMgr.getFollowUpSnaps(
                    snapDfn,
                    remoteForSchedule
                );

                /*
                 * no flux-loop needed here, because neither the for-loops nor backupInfoMgr.getFollowUpSnaps remove
                 * queueItems from any queue
                 */
                for (Entry<QueueItem, TreeSet<Node>> entry : followUpSnaps.entrySet())
                {
                    QueueItem queueItem = entry.getKey();
                    for (Node currentNode : entry.getValue())
                    {
                        if (backupCrtHandler.getFreeShippingSlots(currentNode) > 0)
                        {
                            cleanupFlux = cleanupFlux.concatWith(startFollowUpShippings(queueItem, currentNode));
                        }
                    }
                }
                // If the previous loop didn't fill all shipping slots of this node, start more shipments here
                Node node = peerProvider.get().getNode();
                if (backupCrtHandler.getFreeShippingSlots(node) > 0)
                {
                    cleanupFlux = cleanupFlux.concatWith(
                        startMultipleQueuedShippings(
                            node,
                            new IteratorFromBackupNodeQueue(node, backupInfoMgr, peerAccCtx.get())
                        )
                    );
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
        return cleanupFlux;
    }

    private Flux<ApiCallRc> startFollowUpShippings(QueueItem queueItem, Node currentNode)
        throws AccessDeniedException
    {
        // needs extra method because checkstyle is too stupid to realize the return is in a lambda and does not affect
        // the for-loop
        ExceptionThrowingIterator<QueueItem, AccessDeniedException> nextItem = new IteratorFromSingleItem(queueItem);
        return startQueuedShippings(currentNode, nextItem);
    }

    /**
     * Calls startQueuedShippings until the given node either has no free shipping slots left, or the supplier stops
     * returning items.
     */
    public Flux<ApiCallRc> startMultipleQueuedShippings(
        Node node,
        ExceptionThrowingIterator<QueueItem, AccessDeniedException> nextItem
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Start multiple queued shippings",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> startMultipleQueuedShippingsInTransaction(node, nextItem)
        );
    }

    private Flux<ApiCallRc> startMultipleQueuedShippingsInTransaction(
        Node node,
        ExceptionThrowingIterator<QueueItem, AccessDeniedException> nextItem
    )
    {
        Flux<ApiCallRc> flux;
        try
        {
            if (backupCrtHandler.getFreeShippingSlots(node) > 0 && nextItem.hasNext())
            {
                flux = startQueuedShippings(node, nextItem).concatWith(startMultipleQueuedShippings(node, nextItem));
            }
            else
            {
                flux = Flux.empty();
            }
        }
        catch (AccessDeniedException exc)
        {
            // peerAccessContext comes from stlt which should be privileged
            throw new ImplementationError(exc);
        }
        return flux;
    }

    /**
     * Starts a shipping for the next QueueItem from the supplier or queues it again if it can't be started at the
     * moment, until one shipping was started on the given node or the supplier stops returning items.
     */
    private Flux<ApiCallRc> startQueuedShippings(
        Node node,
        ExceptionThrowingIterator<QueueItem, AccessDeniedException> nextItem
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Start queued shippings",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> startQueuedShippingsInTransaction(node, nextItem)
        );
    }

    private Flux<ApiCallRc> startQueuedShippingsInTransaction(
        Node node,
        ExceptionThrowingIterator<QueueItem, AccessDeniedException> nextItem
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        try
        {
            /*
             * make sure to check free shipping slots before calling nextItem.next(), so that we don't consume an extra
             * item from the queue
             */
            if (backupCrtHandler.getFreeShippingSlots(node) > 0)
            {
                QueueItem next = nextItem.next();
                /*
                 * While prevSnapDfn may be null here (indicating a full backup should be made), a new base snapshot
                 * needs to be decided upon if it was deleted while in the queue.
                 * In case of an l2l-shipping always get a new prevSnap, since the snap could have been deleted on the
                 * target side as well
                 */
                if (next != null)
                {
                    if (next.alreadyStartedOn != null)
                    {
                        /*
                         * although there should be no case where nextItem is not a IteratorFromSingleItem when we get
                         * here, having this in case it does happen should do no harm
                         */
                        if (nextItem.hasNext())
                        {
                            flux = flux.concatWith(startQueuedShippingsInTransaction(node, nextItem));
                        }
                    }
                    else
                    {
                        /*
                         * next.alreadyStartedOn can always be set to the current node, since a) the value itself is
                         * currently not used and b) in the case where the value might be useful, we have a queueItem
                         * that came from getFullowUpShippings, which means we know its prevSnap is valid and it will
                         * therefore be started on this specific node.
                         */
                        next.alreadyStartedOn = node;
                        if (next.remote instanceof S3Remote)
                        {
                            SnapshotDefinition prevSnapDfn = next.prevSnapDfn;
                            /*
                             * while prevSnapDfn may be null here (indicating a full backup should be made), a new base
                             * snapshot needs to be decided upon if it was deleted while in the queue
                             */
                            Node nodeForShipping = node;
                            boolean needsNewPrefSnapDfn = false;
                            if (prevSnapDfn != null)
                            {
                                if (
                                    prevSnapDfn.isDeleted() ||
                                        prevSnapDfn.getFlags()
                                            .isSet(peerAccCtx.get(), SnapshotDefinition.Flags.DELETE) ||
                                        prevSnapDfn.getFlags()
                                            .isUnset(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED)
                                )
                                {
                                    /*
                                     * This method assumes that the prevSnapDfn already has the SHIPPED flag set. Should
                                     * this not be the case, a new prevSnap is needed since allowing the shipping to
                                     * start
                                     * based on an unfinished shipping is simply a bad idea
                                     */
                                    needsNewPrefSnapDfn = true;
                                }
                                else
                                {
                                    Snapshot snap = prevSnapDfn.getSnapshot(peerAccCtx.get(), node.getName());
                                    needsNewPrefSnapDfn = snap == null ||
                                        snap.isDeleted() ||
                                        snap.getFlags().isSet(peerAccCtx.get(), Snapshot.Flags.DELETE);
                                }
                            }
                            if (needsNewPrefSnapDfn)
                            {
                                prevSnapDfn = backupCrtHandler.getIncrementalBase(
                                    next.snapDfn.getResourceDefinition(),
                                    next.remote,
                                    true,
                                    true
                                );
                                if (
                                    prevSnapDfn != null && prevSnapDfn.getSnapshot(
                                        peerAccCtx.get(),
                                        node.getName()
                                    ) == null
                                )
                                {
                                    // if the current node does not have the new prevSnap, a new node needs to be chosen
                                    // as
                                    // well
                                    boolean queueAnyways = prevSnapDfn.getFlags()
                                        .isUnset(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED);
                                    // will be null if snap gets queued in the method
                                    nodeForShipping = backupCrtHandler.getNodeForBackupOrQueue(
                                        next.snapDfn.getResourceDefinition(),
                                        prevSnapDfn,
                                        next.snapDfn,
                                        next.remote,
                                        next.preferredNode,
                                        new ApiCallRcImpl(),
                                        queueAnyways,
                                        null
                                    );
                                }
                            }
                            if (nodeForShipping != null)
                            {
                                try
                                {
                                    // call the "...InTransaction" directly to make sure flags are set immediately
                                    flux = flux.concatWith(
                                        backupCrtHandler.startShippingInTransaction(
                                            next.snapDfn,
                                            nodeForShipping,
                                            next.remote,
                                            prevSnapDfn,
                                            new ApiCallRcImpl()
                                        )
                                    );
                                }
                                catch (ApiAccessDeniedException exc)
                                {
                                    // peerAccessContext comes from stlt which should be privileged
                                    throw new ImplementationError(exc);
                                }
                            }
                            if (!node.equals(nodeForShipping) && nextItem.hasNext())
                            {
                                // s3 can call inTransaction because it does not need to wait
                                flux = flux.concatWith(startQueuedShippingsInTransaction(node, nextItem));
                            }
                        }
                        else if (next.remote instanceof LinstorRemote)
                        {
                            Set<String> srcSnapDfnUuids = new HashSet<>();
                            for (
                                SnapshotDefinition snapDfn : next.snapDfn.getResourceDefinition()
                                    .getSnapshotDfns(sysCtx)
                            )
                            {
                                if (!snapDfn.getAllSnapshots(sysCtx).isEmpty())
                                {
                                    srcSnapDfnUuids.add(snapDfn.getUuid().toString());
                                }
                            }
                            flux = Flux.merge(
                                restClient.sendPrevSnapRequest(
                                    new BackupShippingRequestPrevSnap(
                                        LinStor.VERSION_INFO_PROVIDER.getSemanticVersion(),
                                        next.l2lData.srcClusterId,
                                        next.l2lData.dstRscName,
                                        srcSnapDfnUuids,
                                        next.l2lData.dstNodeName
                                    ),
                                    (LinstorRemote) next.remote,
                                    sysCtx
                                )
                                    .map(
                                        resp -> scopeRunner.fluxInTransactionalScope(
                                            "Backup shipping L2L: start queued shipping",
                                            lockGuardFactory.create()
                                                .read(LockObj.NODES_MAP)
                                                .write(LockObj.RSC_DFN_MAP)
                                                .buildDeferred(),
                                            () -> startQueuedL2LShippingInTransaction(node, nextItem, next, resp)
                                        )
                                    )
                            );
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException | InvalidNameException exc)
        {
            // peerAccessContext comes from stlt which should be privileged
            throw new ImplementationError(exc);
        }
        return flux;
    }

    private Flux<ApiCallRc> startQueuedL2LShippingInTransaction(
        Node node,
        ExceptionThrowingIterator<QueueItem, AccessDeniedException> nextItem,
        QueueItem next,
        BackupShippingResponsePrevSnap resp
    ) throws ImplementationError
    {
        Flux<ApiCallRc> ret;
        Node l2lNodeForShipping = null;
        try
        {
            if (!resp.canReceive)
            {
                ret = Flux.just(resp.responses);
            }
            else
            {
                next.l2lData.resetData = resp.resetData;
                next.l2lData.dstBaseSnapName = resp.dstBaseSnapName;
                next.l2lData.dstActualNodeName = resp.dstActualNodeName;
                SnapshotDefinition l2lPrevSnapDfn = backupCrtHandler.getIncrementalBaseL2L(
                    next.snapDfn.getResourceDefinition(),
                    resp.prevSnapUuid,
                    next.prevSnapDfn != null
                );
                boolean queueAnyways = l2lPrevSnapDfn != null && l2lPrevSnapDfn.getFlags()
                    .isUnset(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED);
                l2lNodeForShipping = backupCrtHandler.getNodeForBackupOrQueue(
                    next.snapDfn.getResourceDefinition(),
                    l2lPrevSnapDfn,
                    next.snapDfn,
                    next.remote,
                    next.preferredNode,
                    new ApiCallRcImpl(),
                    queueAnyways,
                    next.l2lData
                );
                if (l2lNodeForShipping != null)
                {
                    next.l2lData.srcSnapshot = next.snapDfn.getSnapshot(peerAccCtx.get(), l2lNodeForShipping.getName());
                    next.l2lData.srcNodeName = l2lNodeForShipping.getName().displayValue;
                    ret = backupCrtHandler.startShippingInTransaction(
                        next.snapDfn,
                        l2lNodeForShipping,
                        next.remote,
                        l2lPrevSnapDfn,
                        new ApiCallRcImpl()
                    ).concatWith(
                        scopeRunner.fluxInTransactionalScope(
                            "Backup shipping L2L: Create Stlt-Remote",
                            lockGuardFactory.create()
                                .read(LockObj.NODES_MAP)
                                .write(LockObj.RSC_DFN_MAP)
                                .buildDeferred(),
                            () -> backupL2LSrcHandler.get()
                                .createStltRemoteInTransaction(next.l2lData)
                        )
                    );
                }
                else
                {
                    if (resp.responses != null)
                    {
                        ret = Flux.just(resp.responses);
                    }
                    else
                    {
                        ret = Flux.empty();
                    }
                }
            }
            if (!node.equals(l2lNodeForShipping) && nextItem.hasNext())
            {
                ret = ret.concatWith(startQueuedShippings(node, nextItem));
            }
        }
        catch (AccessDeniedException exc)
        {
            // peerAccessContext comes from stlt which should be privileged
            throw new ImplementationError(exc);
        }
        return ret;
    }

    /**
     * Returns the remote and necessary cleanup-flux dependent on which kind of remote it is.
     */
    private Pair<Flux<ApiCallRc>, AbsRemote> getRemoteForScheduleAndCleanupFlux(
        AbsRemote remote,
        ResourceDefinition rscDfn,
        String snapName,
        boolean success
    )
        throws InvalidKeyException, AccessDeniedException, DatabaseException, InvalidValueException
    {
        Flux<ApiCallRc> cleanupFlux;
        AbsRemote remoteForSchedule;
        if (remote != null)
        {
            if (remote instanceof StltRemote)
            {
                StltRemote stltRemote = (StltRemote) remote;
                cleanupFlux = backupHelper.cleanupStltRemote(stltRemote);
                // get the linstor-remote instead, needed for scheduled shipping
                remoteForSchedule = remoteRepo.get(sysCtx, stltRemote.getLinstorRemoteName());
                if (success)
                {
                    rscDfn.getProps(peerAccCtx.get())
                        .setProp(
                            InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                            snapName,
                            ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" +
                                remoteForSchedule.getName().displayValue
                        );
                }
            }
            else
            {
                remoteForSchedule = remote;
                cleanupFlux = Flux.empty();
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
        return new Pair<>(cleanupFlux, remoteForSchedule);
    }

    public static class IteratorFromBackupNodeQueue
        implements ExceptionThrowingIterator<QueueItem, AccessDeniedException>
    {
        private Node node;
        private final BackupInfoManager backupInfoMgr;
        private final AccessContext accCtx;

        public IteratorFromBackupNodeQueue(Node nodeRef, BackupInfoManager backupInfoMgrRef, AccessContext accCtxRef)
        {
            node = nodeRef;
            backupInfoMgr = backupInfoMgrRef;
            accCtx = accCtxRef;
        }

        @Override
        public boolean hasNext() throws AccessDeniedException
        {
            return backupInfoMgr.getNextFromQueue(accCtx, node, false) != null;
        }

        @Override
        public QueueItem next() throws AccessDeniedException
        {
            return backupInfoMgr.getNextFromQueue(accCtx, node, true);
        }
    }

    private class IteratorFromSingleItem implements ExceptionThrowingIterator<QueueItem, AccessDeniedException>
    {
        private QueueItem item;

        IteratorFromSingleItem(QueueItem itemRef)
        {
            item = itemRef;
        }

        @Override
        public boolean hasNext()
        {
            return item != null;
        }

        @Override
        public QueueItem next() throws AccessDeniedException
        {
            QueueItem ret = item;
            if (ret != null)
            {
                backupInfoMgr.deleteFromQueue(ret.snapDfn, ret.remote);
                item = null;
            }
            return ret;
        }
    }
}
