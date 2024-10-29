package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.backups.BackupNodeQueuesPojo;
import com.linbit.linstor.api.pojo.backups.BackupSnapQueuesPojo;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.BackupInfoManager.QueueItem;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupCreateApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingRestClient;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingRequestPrevSnap;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingResponsePrevSnap;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.SystemConfProtectionRepository;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.ExceptionThrowingIterator;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlBackupQueueInternalCallHandler
{
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final AccessContext sysCtx;
    private final Provider<Peer> peerProvider;
    private final BackupInfoManager backupInfoMgr;
    private final BackupShippingRestClient restClient;
    private final Provider<CtrlBackupL2LSrcApiCallHandler> backupL2LSrcHandler;
    private final CtrlBackupCreateApiCallHandler backupCrtHandler;
    private final NodeRepository nodeRepo;
    private final SystemConfProtectionRepository sysCfgRepo;
    private final CtrlBackupApiHelper backupHelper;

    @Inject
    public CtrlBackupQueueInternalCallHandler(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @SystemContext AccessContext sysCtxRef,
        Provider<Peer> peerProviderRef,
        BackupInfoManager backupInfoMgrRef,
        BackupShippingRestClient restClientRef,
        Provider<CtrlBackupL2LSrcApiCallHandler> backupL2LSrcHandlerRef,
        CtrlBackupCreateApiCallHandler backupCrtHandlerRef,
        NodeRepository nodeRepoRef,
        SystemConfProtectionRepository sysCfgRepoRef,
        CtrlBackupApiHelper backupHelperRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        sysCtx = sysCtxRef;
        peerProvider = peerProviderRef;
        backupInfoMgr = backupInfoMgrRef;
        restClient = restClientRef;
        backupL2LSrcHandler = backupL2LSrcHandlerRef;
        backupCrtHandler = backupCrtHandlerRef;
        nodeRepo = nodeRepoRef;
        sysCfgRepo = sysCfgRepoRef;
        backupHelper = backupHelperRef;

    }

    public Flux<ApiCallRc> handleBackupQueues(
        SnapshotDefinition snapDfn,
        AbsRemote remoteForSchedule,
        @Nullable StltRemote optStltRemote
    ) throws AccessDeniedException
    {
        Flux<ApiCallRc> flux = Flux.empty();
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
        if (snapDfn != null)
        {
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
                        ExceptionThrowingIterator<QueueItem, AccessDeniedException> next = new IteratorFromSingleItem(
                            queueItem
                        );
                        flux = flux.concatWith(startQueuedShippings(currentNode, next));
                    }
                }
            }
        }
        // If the previous loop didn't fill all shipping slots of this node, start more shipments here

        // TODO: do not delete stlt remote when src is done, instead start new task & save it in the stlt-remote. If dst
        // tells us it's done, delete stlt-remote and task, then continue. If task triggers first, delete stlt-remote
        // then and continue - error might still happen, but at this point there isn't really anything we can do.
        // Although it would be possible to ask dst at this point what's taking it so long and then maybe wait a bit
        // more...
        Node node;
        if (optStltRemote != null)
        {
            node = optStltRemote.getNode();
            flux = flux.concatWith(backupHelper.cleanupStltRemote(optStltRemote));
        }
        else
        {
            node = peerProvider.get().getNode();
        }
        // no need to continue with starting queues if the node was deleted
        boolean nodeDeleted = node.isDeleted() || node.getFlags().isSet(peerAccCtx.get(), Node.Flags.DELETE);
        if (!nodeDeleted && backupCrtHandler.getFreeShippingSlots(node) > 0)
        {
            flux = flux.concatWith(
                startMultipleQueuedShippings(
                    node,
                    new IteratorFromBackupNodeQueue(node, backupInfoMgr, peerAccCtx.get())
                )
            );
        }
        return flux;
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
                            flux = startQueuedShippingsInTransaction(node, nextItem);
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
                            flux = handleS3QueueItem(node, nextItem, next);
                        }
                        else if (next.remote instanceof LinstorRemote)
                        {
                            flux = handleL2LQueueItem(node, nextItem, next);
                        }
                        else
                        {
                            throw new ImplementationError(
                                "Unexpected Remote type: " + next.remote.getClass().getSimpleName()
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

    private Flux<ApiCallRc> handleS3QueueItem(
        Node node,
        ExceptionThrowingIterator<QueueItem, AccessDeniedException> nextItem,
        QueueItem current
    ) throws AccessDeniedException, InvalidNameException, ImplementationError
    {
        SnapshotDefinition prevSnapDfn = current.prevSnapDfn;
        Flux<ApiCallRc> flux = Flux.empty();
        // nodeForShipping is the node that the queueItem will be started on, and while in the best case it will be the
        // same as node (the node we want to start shippings on), it might not always be
        Node nodeForShipping = node;
        boolean needsNewPrefSnapDfn = false;
        /*
         * while prevSnapDfn may be null here (indicating a full backup should be made), a new base
         * snapshot needs to be decided upon if it was deleted while in the queue
         */
        if (prevSnapDfn != null)
        {
            if (
                prevSnapDfn.isDeleted() ||
                    prevSnapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.DELETE) ||
                    prevSnapDfn.getFlags().isUnset(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED)
            )
            {
                /*
                 * This method assumes that the prevSnapDfn already has the SHIPPED flag set. Should
                 * this not be the case, a new prevSnap is needed since allowing the shipping to
                 * start based on an unfinished shipping is simply a bad idea
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
                current.snapDfn.getResourceDefinition(),
                current.remote,
                true,
                true
            );
            if (prevSnapDfn != null && prevSnapDfn.getSnapshot(peerAccCtx.get(), node.getName()) == null)
            {
                // if the current node does not have the new prevSnap, a new node needs to be chosen
                // as well
                boolean queueAnyways = prevSnapDfn.getFlags()
                    .isUnset(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED);
                // nodeForShipping will be null if snap gets queued in the method
                nodeForShipping = backupCrtHandler.getNodeForBackupOrQueue(
                    current.snapDfn.getResourceDefinition(),
                    prevSnapDfn,
                    current.snapDfn,
                    current.remote,
                    current.preferredNode,
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
                flux = backupCrtHandler.startShippingInTransaction(
                    current.snapDfn,
                    nodeForShipping,
                    current.remote,
                    prevSnapDfn,
                    new ApiCallRcImpl(),
                    current.preferredNode,
                    current.l2lData
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
        return flux;
    }

    private Flux<ApiCallRc> handleL2LQueueItem(
        Node node,
        ExceptionThrowingIterator<QueueItem, AccessDeniedException> nextItem,
        QueueItem current
    ) throws AccessDeniedException
    {
        Flux<ApiCallRc> flux;
        Set<String> srcSnapDfnUuids = new HashSet<>();
        for (SnapshotDefinition snapDfn : current.snapDfn.getResourceDefinition().getSnapshotDfns(sysCtx))
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
                    current.l2lData.getSrcClusterId(),
                    current.l2lData.getDstRscName(),
                    srcSnapDfnUuids,
                    current.l2lData.getDstNodeName()
                ),
                (LinstorRemote) current.remote,
                sysCtx
            ).map(
                resp -> scopeRunner.fluxInTransactionalScope(
                    "Backup shipping L2L: start queued shipping",
                    lockGuardFactory.create()
                        .read(LockObj.NODES_MAP)
                        .write(LockObj.RSC_DFN_MAP)
                        .buildDeferred(),
                    () -> startQueuedL2LShippingInTransaction(node, nextItem, current, resp)
                )
            )
        );
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
                next.l2lData.setResetData(resp.resetData);
                next.l2lData.setDstBaseSnapName(resp.dstBaseSnapName);
                next.l2lData.setDstActualNodeName(resp.dstActualNodeName);
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
                    next.l2lData.setSrcSnapshot(
                        next.snapDfn.getSnapshot(peerAccCtx.get(), l2lNodeForShipping.getName())
                    );
                    next.l2lData.setSrcNodeName(l2lNodeForShipping.getName().displayValue);
                    final Node nodeForEffectivelyFinal = l2lNodeForShipping;
                    ret = backupCrtHandler.startShippingInTransaction(
                        next.snapDfn,
                        l2lNodeForShipping,
                        next.remote,
                        l2lPrevSnapDfn,
                        new ApiCallRcImpl(),
                        next.preferredNode,
                        next.l2lData
                    )
                        .concatWith(
                            scopeRunner.fluxInTransactionalScope(
                                "Backup shipping L2L: Create Stlt-Remote",
                                lockGuardFactory.create()
                                    .read(LockObj.NODES_MAP)
                                    .write(LockObj.RSC_DFN_MAP)
                                    .buildDeferred(),
                                () -> backupL2LSrcHandler.get()
                                    .createStltRemoteInTransaction(next.l2lData, nodeForEffectivelyFinal)
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

    public Flux<ApiCallRc> maxConcurrentShippingsChangedForCtrl()
    {
        return scopeRunner.fluxInTransactionalScope(
            "BackupsPerNode changed on ctrl",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            this::maxConcurrentShippingsChangedForCtrlInTransaction
        );
    }

    public Flux<ApiCallRc> maxConcurrentShippingsChangedForNode(Node node)
    {
        return scopeRunner.fluxInTransactionalScope(
            "BackupsPerNode changed on node",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> maxConcurrentShippingsChangedForNodeInTransaction(node)
        );
    }

    /**
     * Starts new shipments or deletes queues depending on the changes to the prop
     * "BackupShipping/MaxConcurrentBackupsPerNode".
     * This method needs to be called in a scope with all the locks needed for starting a shipping, since in case
     * a shipping needs to be started startShippingInTransaction will be called directly.
     */
    private Flux<ApiCallRc> maxConcurrentShippingsChangedForNodeInTransaction(Node node)
        throws AccessDeniedException
    {
        Flux<ApiCallRc> flux = Flux.empty();
        PriorityProps prioProps = new PriorityProps(
            node.getProps(peerAccCtx.get()),
            sysCfgRepo.getCtrlConfForView(peerAccCtx.get())
        );
        String maxBackups = prioProps.getProp(
            ApiConsts.KEY_MAX_CONCURRENT_BACKUPS_PER_NODE,
            ApiConsts.NAMESPC_BACKUP_SHIPPING
        );
        if (maxBackups != null && !maxBackups.isEmpty() && Integer.parseInt(maxBackups) == 0)
        {
            flux = backupCrtHandler.deleteNodeQueueAndReQueueSnapsIfNeeded(node);
        }
        else
        {
            if (backupCrtHandler.getFreeShippingSlots(node) > 0)
            {
                flux = startMultipleQueuedShippings(
                    node,
                    new IteratorFromBackupNodeQueue(node, backupInfoMgr, peerAccCtx.get())
                );
            }
        }
        return flux;
    }

    /**
     * Starts new shipments or deletes queues depending on the changes to the prop
     * "BackupShipping/MaxConcurrentBackupsPerNode".
     * This method needs to be called in a scope with all the locks needed for starting a shipping, since in case
     * a shipping needs to be started startShippingInTransaction will be called directly.
     */
    private Flux<ApiCallRc> maxConcurrentShippingsChangedForCtrlInTransaction() throws AccessDeniedException
    {
        Flux<ApiCallRc> flux = Flux.empty();
        AccessContext accCtx = peerAccCtx.get();
        Collection<Node> nodes = nodeRepo.getMapForView(accCtx).values();
        List<Node> nodesToClear = new ArrayList<>();
        List<Node> nodesToStart = new ArrayList<>();
        int toClearCt = 0;
        for (Node node : nodes)
        {
            ReadOnlyProps nodeProps = node.getProps(accCtx);
            PriorityProps prioProps = new PriorityProps(
                nodeProps,
                sysCfgRepo.getCtrlConfForView(accCtx)
            );
            String maxBackups = prioProps.getProp(
                ApiConsts.KEY_MAX_CONCURRENT_BACKUPS_PER_NODE,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
            String nodeMaxBackups = nodeProps.getProp(
                ApiConsts.KEY_MAX_CONCURRENT_BACKUPS_PER_NODE,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
            if (maxBackups != null && !maxBackups.isEmpty() && Integer.parseInt(maxBackups) == 0)
            {
                toClearCt++;
                if (nodeMaxBackups == null)
                {
                    // since prop is not set on node level, the value changed and therefore the node-queue needs
                    // to be deleted
                    nodesToClear.add(node);
                }
            }
            else
            {
                if (nodeMaxBackups == null)
                {
                    // since prop is not set on node level, the value changed and therefore checking whether new
                    // shippings can be started is necessary
                    nodesToStart.add(node);
                }
            }
        }
        if (toClearCt == nodes.size())
        {
            // clear everything, no need to try and add to other queue, since all shipping should be stopped
            backupInfoMgr.deleteAllQueues();
        }
        else
        {
            for (Node node : nodesToClear)
            {
                flux = flux.concatWith(backupCrtHandler.deleteNodeQueueAndReQueueSnapsIfNeeded(node));
            }
            for (Node node : nodesToStart)
            {
                /*
                 * This is just a pre-filter; since the result of getFreeShippingSlots() changes due to the results of
                 * the following flux(es), we cannot be certain that the result of this if is still valid when we get to
                 * actually trying to start shipments, which is why there are additional checks for
                 * getFreeShippingSlots() later on
                 */
                if (backupCrtHandler.getFreeShippingSlots(node) > 0)
                {
                    flux = flux.concatWith(
                        startMultipleQueuedShippings(
                            node,
                            new IteratorFromBackupNodeQueue(node, backupInfoMgr, accCtx)
                        )
                    );
                }
            }
        }
        return flux;
    }

    public List<BackupSnapQueuesPojo> listSnapQueues(
        List<String> nodesRef,
        List<String> snapshotsRef,
        List<String> resourcesRef,
        List<String> remotesRef
    )
    {
        List<BackupSnapQueuesPojo> ret = new ArrayList<>();
        Predicate<QueueItem> snapFilter = createFilter(snapshotsRef, item -> item.snapDfn.getName().displayValue);
        Predicate<QueueItem> rscFilter = createFilter(
            resourcesRef,
            item -> item.snapDfn.getResourceName().displayValue
        );
        Predicate<QueueItem> remoteFilter = createFilter(remotesRef, item -> item.remote.getName().displayValue);
        Predicate<String> nodeFilter = createFilter(nodesRef, Function.identity());

        Map<QueueItem, Set<String>> queueMap = getSnapToNodeQueueMap();
        for (Entry<QueueItem, Set<String>> queue : queueMap.entrySet())
        {
            QueueItem item = queue.getKey();
            boolean matches = snapFilter.test(item) || rscFilter.test(item) || remoteFilter.test(item);
            if (!matches)
            {
                for (String node : queue.getValue())
                {
                    if (nodeFilter.test(node))
                    {
                        matches = true;
                        break;
                    }
                }
            }
            BackupSnapQueuesPojo snapQueuesPojo = createSnapQueuesPojo(queue.getValue(), item, matches);
            if (snapQueuesPojo != null)
            {
                ret.add(snapQueuesPojo);
            }
        }
        return ret;
    }

    /**
     * Compiles a map of QueueItem to a set of node names (as Strings) from two sources:
     * backupInfoMgr.getSnapToNodeQueues() and backupInfoMgr.getPrevNodeUndecidedQueue().
     * All nodes from the former list are only included if the user has at least VIEW access; the QueueItems from the
     * latter get ApiConsts.VAL_NODE_UNDECIDED set as a node name.
     * In both cases the access rights of QueueItems need to be checked at a later point.
     */
    private Map<QueueItem, Set<String>> getSnapToNodeQueueMap()
    {
        Map<QueueItem, Set<String>> queueMap = new HashMap<>();
        Map<Node, Boolean> cachedAccessAllowed = new HashMap<>();
        for (Entry<QueueItem, Set<Node>> queue : backupInfoMgr.getSnapToNodeQueues())
        {
            Set<String> nodeStrs = new HashSet<>();
            queueMap.put(queue.getKey(), nodeStrs);
            for (Node node : queue.getValue())
            {
                Boolean access = cachedAccessAllowed.get(node);
                if (access == null)
                {
                    access = node.getObjProt().queryAccess(peerAccCtx.get()) != null;
                    cachedAccessAllowed.put(node, access);
                }
                if (access)
                {
                    nodeStrs.add(node.getName().displayValue);
                }
            }
        }
        for (QueueItem item : backupInfoMgr.getPrevNodeUndecidedQueue())
        {
            // for queueItems where it is still unknown on which nodes they can be shipped
            queueMap.put(item, Collections.singleton(ApiConsts.VAL_NODE_UNDECIDED));
        }
        return queueMap;
    }

    private BackupSnapQueuesPojo createSnapQueuesPojo(
        Set<String> queuedOnNodes,
        QueueItem item,
        boolean matches
    )
    {
        BackupSnapQueuesPojo ret = null;
        // not null means at least VIEW access, snapDfn gets checked in queueItemToPojo
        if (matches && item.remote.getObjProt().queryAccess(peerAccCtx.get()) != null)
        {
            List<BackupNodeQueuesPojo> nodes = new ArrayList<>();
            for (String node : queuedOnNodes)
            {
                nodes.add(new BackupNodeQueuesPojo(node, null));
            }
            if (!nodes.isEmpty())
            {
                ret = queueItemToPojo(item, nodes);
            }
        }
        return ret;
    }

    public List<BackupNodeQueuesPojo> listNodeQueues(
        List<String> nodesRef,
        List<String> snapshotsRef,
        List<String> resourcesRef,
        List<String> remotesRef
    )
    {
        List<BackupNodeQueuesPojo> ret = new ArrayList<>();
        Predicate<QueueItem> snapFilter = createFilter(snapshotsRef, item -> item.snapDfn.getName().displayValue);
        Predicate<QueueItem> rscFilter = createFilter(
            resourcesRef,
            item -> item.snapDfn.getResourceName().displayValue
        );
        Predicate<QueueItem> remoteFilter = createFilter(remotesRef, item -> item.remote.getName().displayValue);
        Predicate<String> nodeFilter = createFilter(nodesRef, Function.identity());

        Map<String, Set<QueueItem>> queueMap = getNodeToSnapQueueMap();

        for (Entry<String, Set<QueueItem>> queue : queueMap.entrySet())
        {
            String node = queue.getKey();
            boolean matches = false;
            if (!nodeFilter.test(node))
            {
                for (QueueItem item : queue.getValue())
                {
                    matches = snapFilter.test(item) || rscFilter.test(item) || remoteFilter.test(item);
                    if (matches)
                    {
                        matches = true;
                        break;
                    }
                }
            }
            else
            {
                matches = true;
            }
            List<BackupSnapQueuesPojo> items = createQueueItemPojoList(queue.getValue(), matches);
            if (!items.isEmpty())
            {
                ret.add(new BackupNodeQueuesPojo(node, items));
            }
        }
        return ret;
    }

    /**
     * Compiles a map of node names (as Strings) to a set of QueueItems from two sources:
     * backupInfoMgr.getNodeToSnapQueues() and backupInfoMgr.getPrevNodeUndecidedQueue().
     * All nodes from the former list are only included if the user has at least VIEW access; the QueueItems from the
     * latter are added with ApiConsts.VAL_NODE_UNDECIDED set as their node name.
     * In both cases the access rights of QueueItems need to be checked at a later point.
     */
    private Map<String, Set<QueueItem>> getNodeToSnapQueueMap()
    {
        Map<String, Set<QueueItem>> queueMap = new HashMap<>();
        Map<Node, Boolean> cachedAccessAllowed = new HashMap<>();
        for (Entry<Node, Set<QueueItem>> queue : backupInfoMgr.getNodeToSnapQueues())
        {
            Node node = queue.getKey();
            Set<QueueItem> items = queue.getValue();
            Boolean access = cachedAccessAllowed.get(node);
            if (access == null)
            {
                access = node.getObjProt().queryAccess(peerAccCtx.get()) != null;
                cachedAccessAllowed.put(node, access);
            }
            if (access)
            {
                queueMap.put(node.getName().displayValue, items);
            }
        }
        // for queueItems where it is still unknown on which nodes they can be shipped
        queueMap.put(ApiConsts.VAL_NODE_UNDECIDED, backupInfoMgr.getPrevNodeUndecidedQueue());
        return queueMap;
    }

    private List<BackupSnapQueuesPojo> createQueueItemPojoList(Set<QueueItem> queueItems, boolean matches)
    {
        List<BackupSnapQueuesPojo> items = new ArrayList<>();
        if (matches)
        {
            for (QueueItem item : queueItems)
            {
                // not null means at least VIEW access
                if (item.remote.getObjProt().queryAccess(peerAccCtx.get()) != null)
                {
                    BackupSnapQueuesPojo queueItemPojo = queueItemToPojo(item, null);
                    // if no access to snapDfn, queueItemPojo is null
                    if (queueItemPojo != null)
                    {
                        items.add(queueItemPojo);
                    }
                }
            }
        }
        return items;
    }

    private BackupSnapQueuesPojo queueItemToPojo(QueueItem item, List<BackupNodeQueuesPojo> nodes)
    {
        BackupSnapQueuesPojo ret;
        String startTimeStr = null;
        try
        {
            startTimeStr = item.snapDfn.getSnapDfnProps(peerAccCtx.get())
                .getProp(InternalApiConsts.KEY_BACKUP_START_TIMESTAMP, ApiConsts.NAMESPC_BACKUP_SHIPPING);
            boolean inc = item.prevSnapDfn != null;
            ret = new BackupSnapQueuesPojo(
                item.snapDfn.getResourceName().displayValue,
                item.snapDfn.getName().displayValue,
                item.remote.getName().displayValue,
                inc,
                inc ? item.prevSnapDfn.getName().displayValue : null,
                (startTimeStr == null || startTimeStr.isEmpty()) ? null : Long.parseLong(startTimeStr),
                item.preferredNode,
                nodes
            );
        }
        catch (AccessDeniedException exc)
        {
            ret = null;
        }
        return ret;
    }

    private <T> Predicate<T> createFilter(List<String> list, Function<T, String> mapper)
    {
        Predicate<T> ret;
        if (list != null && !list.isEmpty())
        {
            Set<String> lowerList = new HashSet<>();
            for (String item : list)
            {
                lowerList.add(item.toLowerCase());
            }
            ret = item -> lowerList.contains(mapper.apply(item).toLowerCase());
        }
        else
        {
            ret = ignored -> true;
        }
        return ret;
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
