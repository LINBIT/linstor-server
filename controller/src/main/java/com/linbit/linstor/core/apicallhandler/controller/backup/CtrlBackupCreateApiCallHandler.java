package com.linbit.linstor.core.apicallhandler.controller.backup;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.BackupInfoManager.QueueItem;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupApiHelper.S3ObjectInfo;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingSrcData;
import com.linbit.linstor.core.apicallhandler.controller.backup.nodefinder.BackupNodeFinder;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.req.CreateMultiSnapRequest;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.repository.SystemConfProtectionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;
import com.linbit.utils.TimeUtils;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import reactor.core.publisher.Flux;
import reactor.util.context.Context;

@Singleton
public class CtrlBackupCreateApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlSecurityObjects ctrlSecObj;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSnapshotCrtHelper snapCrtHelper;
    private final Provider<AccessContext> peerAccCtx;
    private final SystemConfProtectionRepository sysCfgRepo;
    private final AccessContext sysCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlSnapshotCrtApiCallHandler snapshotCrtHandler;
    private final ErrorReporter errorReporter;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlBackupApiHelper backupHelper;
    private final BackupNodeFinder backupNodeFinder;

    @Inject
    public CtrlBackupCreateApiCallHandler(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlSecurityObjects ctrlSecObjRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSnapshotCrtHelper snapCrtHelperRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @SystemContext AccessContext sysCtxRef,
        SystemConfProtectionRepository sysCfgRepoRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSnapshotCrtApiCallHandler snapshotCrtHandlerRef,
        ErrorReporter errorReporterRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlBackupApiHelper backupHelperRef,
        BackupNodeFinder backupNodeFinderRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlSecObj = ctrlSecObjRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        snapCrtHelper = snapCrtHelperRef;
        peerAccCtx = peerAccCtxRef;
        sysCtx = sysCtxRef;
        sysCfgRepo = sysCfgRepoRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        snapshotCrtHandler = snapshotCrtHandlerRef;
        errorReporter = errorReporterRef;
        backupInfoMgr = backupInfoMgrRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        backupHelper = backupHelperRef;
        backupNodeFinder = backupNodeFinderRef;

    }

    public Flux<ApiCallRc> createBackup(
        String rscNameRef,
        String snapNameRef,
        String remoteNameRef,
        String nodeNameRef,
        String scheduleNameRef,
        boolean incremental,
        boolean runInBackgroundRef
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Prepare backup",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> backupSnapshot(
                rscNameRef,
                remoteNameRef,
                nodeNameRef,
                snapNameRef,
                LocalDateTime.now(),
                incremental,
                RemoteType.S3,
                scheduleNameRef,
                runInBackgroundRef,
                null,
                null
            ).objA
        );
    }

    /**
     * Starts a backup shipping.<br/>
     * More detailed order of things:
     * <ul>
     * <li>Generates a snapName if needed</li>
     * <li>Checks encryption</li>
     * <li>Makes sure no other shipping of this rsc is currently running</li>
     * <li>Makes sure an incremental backup is made if allowed</li>
     * <li>Chooses a node</li>
     * <li>Creates the snapshot on all nodes</li>
     * <li>Makes sure metadata is handled the same over all volumes</li>
     * <li>Saves the drbd-node-ids since they will be needed for a restore</li>
     * <li>Sets all flags and props needed for the stlt to start the shipping</li>
     * </ul>
     *
     * @param runInBackgroundRef
     */
    Pair<Flux<ApiCallRc>, Snapshot> backupSnapshot(
        String rscNameRef,
        String remoteName,
        String nodeName,
        String snapNameRef,
        LocalDateTime nowRef,
        boolean allowIncremental,
        RemoteType remoteTypeRef,
        String scheduleNameRef,
        boolean runInBackgroundRef,
        @Nullable String prevSnapDfnUuid,
        @Nullable BackupShippingSrcData l2lData
    )
    {
        String snapName = snapNameRef;

        try
        {
            ApiCallRcImpl responses = new ApiCallRcImpl();

            if (snapName == null || snapName.isEmpty())
            {
                snapName = BackupShippingUtils.generateBackupName(nowRef);
                responses.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.MASK_INFO,
                        "Generated snapshot name for backup of resource" + rscNameRef + " to remote " + remoteName
                    )
                );
            }
            // test if master key is unlocked
            if (!ctrlSecObj.areAllSet())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_CRYPT_PASSPHRASE,
                        "Backup shipping requires a set up encryption. Please use 'linstor encryption " +
                            "create-passphrase' or '... enter-passphrase'"
                    )
                );
            }
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
            AbsRemote remote;
            SnapshotDefinition prevSnapDfn = null;
            if (remoteTypeRef.equals(RemoteType.S3))
            {
                // check if encryption is possible
                backupHelper.getLocalMasterKey();

                remote = backupHelper.getS3Remote(remoteName);
                prevSnapDfn = getIncrementalBase(rscDfn, remote, allowIncremental, false);
            }
            else if (remoteTypeRef.equals(RemoteType.LINSTOR))
            {
                remote = backupHelper.getL2LRemote(remoteName);
                prevSnapDfn = getIncrementalBaseL2L(rscDfn, prevSnapDfnUuid, allowIncremental);
            }
            else
            {
                throw new ImplementationError("remote type " + remoteTypeRef + " not allowed");
            }

            SnapshotDefinition snapDfn = snapCrtHelper
                .createSnapshots(Collections.emptyList(), rscDfn.getName().displayValue, snapName, responses);
            setBackupSnapDfnFlagsAndProps(snapDfn, scheduleNameRef, nowRef);
            /*
             * See if the previous snap has already finished shipping. If it hasn't, the current snap must be queued to
             * prevent two consecutive shippings from happening at the same time
             */
            boolean queueAnyways = prevSnapDfn != null &&
                prevSnapDfn.getFlags().isUnset(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED);
            Node chosenNode = getNodeForBackupOrQueue(
                rscDfn,
                prevSnapDfn,
                snapDfn,
                remote,
                nodeName,
                responses,
                queueAnyways,
                l2lData
            );

            List<Integer> nodeIds = new ArrayList<>();
            DrbdRscDfnData<Resource> rscDfnData = rscDfn.getLayerData(
                peerAccCtx.get(),
                DeviceLayerKind.DRBD,
                RscLayerSuffixes.SUFFIX_DATA
            );
            if (rscDfnData != null)
            {
                for (DrbdRscData<Resource> rscData : rscDfnData.getDrbdRscDataList())
                {
                    if (!rscData.isDiskless(sysCtx))
                    {
                        /*
                         * diskless nodes do reserve a node-id for themselves, but the peer-slot is not used in the
                         * metadata of diskfull peers
                         */
                        nodeIds.add(rscData.getNodeId().value);
                    }
                }
            }
            setStartBackupProps(snapDfn, remoteName, nodeIds);

            if (remote instanceof S3Remote)
            {
                // only do this for s3, l2l does it on its own later on
                setIncrementalDependentProps(snapDfn, prevSnapDfn, remoteName, scheduleNameRef);
            }
            rscDfn.getProps(peerAccCtx.get())
                .setProp(
                    InternalApiConsts.KEY_BACKUP_LAST_STARTED_OR_QUEUED,
                    snapName,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" +
                        remote.getName().displayValue
                );
            ctrlTransactionHelper.commit();

            responses.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_INFO, "Shipping of resource " + rscNameRef + " to remote " + remoteName +
                        " in progress."
                ).putObjRef(ApiConsts.KEY_SNAPSHOT, snapName).build()
            );
            Flux<ApiCallRc> flux = snapshotCrtHandler.postCreateSnapshot(snapDfn, runInBackgroundRef)
                .concatWith(Flux.<ApiCallRc>just(responses));
            if (chosenNode != null)
            {
                flux = flux.concatWith(
                    startShipping(snapDfn, chosenNode, remote, prevSnapDfn, responses, nodeName, l2lData)
                );
            }
            return new Pair<>(
                flux,
                chosenNode != null ? snapDfn.getSnapshot(peerAccCtx.get(), chosenNode.getName()) : null
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "prepare backup shpping", ApiConsts.FAIL_ACC_DENIED_RSC);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        catch (InvalidKeyException | InvalidNameException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    Flux<ApiCallRc> startShipping(
        SnapshotDefinition snapDfn,
        Node node,
        AbsRemote remote,
        SnapshotDefinition prevSnapDfn,
        ApiCallRcImpl responses,
        @Nullable String optPrefNodeName,
        @Nullable BackupShippingSrcData optL2LData
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Start backup shipping",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> startShippingInTransaction(
                snapDfn,
                node,
                remote,
                prevSnapDfn,
                responses,
                optPrefNodeName,
                optL2LData
            )
        );
    }

    public Flux<ApiCallRc> startShippingInTransaction(
        SnapshotDefinition snapDfn,
        Node node,
        AbsRemote remote,
        SnapshotDefinition prevSnapDfn,
        ApiCallRcImpl responsesRef,
        @Nullable String optPrefNodeName,
        @Nullable BackupShippingSrcData optL2LData
    )
    {
        try
        {
            backupHelper.ensureShippingToRemoteAllowed(remote);
            Flux<ApiCallRc> flux;
            // doublecheck free shipping slots, if none are free, queue
            if (getFreeShippingSlots(node) > 0)
            {
                snapDfn.getFlags()
                    .enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING, SnapshotDefinition.Flags.BACKUP);
                if (remote instanceof S3Remote)
                {
                    snapDfn.getSnapDfnProps(peerAccCtx.get())
                        .setProp(
                            InternalApiConsts.KEY_BACKUP_SRC_NODE + "/" + remote.getName(),
                            node.getName().displayValue,
                            ApiConsts.NAMESPC_BACKUP_SHIPPING
                        );
                }
                else if (remote instanceof LinstorRemote)
                {
                    snapDfn.getSnapDfnProps(peerAccCtx.get())
                        .setProp(
                            InternalApiConsts.KEY_BACKUP_SRC_NODE + "/" + remote.getName() +
                                "/" + snapDfn.getResourceName().displayValue,
                            node.getName().displayValue,
                            ApiConsts.NAMESPC_BACKUP_SHIPPING
                        );
                }
                // now that it is decided that this node will do the shipping, see if any snapDfn can be moved to the
                // normal queues
                QueueItem item = backupInfoMgr.getItemFromPrevNodeUndecidedQueue(snapDfn, remote);
                if (item != null)
                {
                    // return value is ignored since queueAnyways is set
                    getNodeForBackupOrQueue(
                        snapDfn.getResourceDefinition(),
                        snapDfn,
                        item.snapDfn,
                        item.remote,
                        item.preferredNode,
                        responsesRef,
                        true, // always queue to avoid simultaneous shippings of consecutive backups
                        item.l2lData
                    );
                }

                Snapshot snap = snapDfn.getSnapshot(peerAccCtx.get(), node.getName());
                if (remote instanceof S3Remote)
                {
                    // l2l does not need this to be set...
                    snap.getFlags()
                        .enableFlags(peerAccCtx.get(), Snapshot.Flags.BACKUP_SOURCE);
                    snap.setTakeSnapshot(peerAccCtx.get(), true);
                    snap.getSnapProps(peerAccCtx.get())
                        .setProp(
                            InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                            remote.getName().displayValue,
                            ApiConsts.NAMESPC_BACKUP_SHIPPING
                        );
                }
                if (prevSnapDfn != null)
                {
                    snap.getSnapProps(peerAccCtx.get())
                        .setProp(
                            InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                            prevSnapDfn.getName().displayValue,
                            ApiConsts.NAMESPC_BACKUP_SHIPPING
                        );
                }

                ctrlTransactionHelper.commit();
                flux = ctrlSatelliteUpdateCaller.updateSatellites(snapDfn, CtrlSatelliteUpdateCaller.notConnectedWarn())
                    .transform(
                        responses -> CtrlResponseUtils
                            .combineResponses(
                                errorReporter, responses, snapDfn.getResourceName(), "Started shipping of resource {1}")
                    )
                    .concatWith(
                        snapshotCrtHandler.removeInProgressSnapshots(
                            new CreateMultiSnapRequest(peerAccCtx.get(), snapDfn)
                        )
                    );
            }
            else
            {
                // we ignore any chance that the shipping could be started on a different node and instead queue
                // anyways, for simplicity's sake
                getNodeForBackupOrQueue(
                    snapDfn.getResourceDefinition(),
                    prevSnapDfn,
                    snapDfn,
                    remote,
                    optPrefNodeName,
                    responsesRef,
                    true, // queue anyways
                    optL2LData
                );
                flux = Flux.empty();
            }
            return flux;
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "start backup shpping", ApiConsts.FAIL_ACC_DENIED_RSC);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * Deletes the queue of the given node and also re-queues any snapDfns that were only in its queue (or starts them
     * if possible).
     */
    public void deleteNodeQueue(Peer peer)
    {
        Node node = peer.getNode();
        if (!node.isDeleted())
        {
            Flux<ApiCallRc> flux = deleteNodeQueueAndReQueueSnapsIfNeeded(peer.getNode());
            Thread thread = new Thread(() ->
                flux.contextWrite(
                    Context.of(
                        AccessContext.class,
                        peer.getAccessContext(),
                        Peer.class,
                        peer,
                        ApiModule.API_CALL_NAME,
                        "delete node queue"
                    )
                ).subscribe(ignoredResults -> { }, errorReporter::reportError)
            );
            thread.start();
        }
    }

    public Flux<ApiCallRc> deleteNodeQueueAndReQueueSnapsIfNeeded(String nodeName)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Delete node queue",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> deleteNodeQueueAndReQueueSnapsIfNeededInTransaction(ctrlApiDataLoader.loadNode(nodeName, false))
        );
    }

    /*
     * Deletes the queue of the given node and checks if any snapDfn is not being shipped anymore because of it.
     * If so, finds new nodes to ship that snapDfn from and either queues it there or starts the shipping.
     */
    public Flux<ApiCallRc> deleteNodeQueueAndReQueueSnapsIfNeeded(Node node)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Delete node queue",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> deleteNodeQueueAndReQueueSnapsIfNeededInTransaction(node)
        );
    }

    private Flux<ApiCallRc> deleteNodeQueueAndReQueueSnapsIfNeededInTransaction(Node node)
    {
        Flux<ApiCallRc> flux = Flux.empty();
        if (node != null && !node.isDeleted())
        {
            List<QueueItem> itemsToReQueue = backupInfoMgr.deleteFromQueue(node);
            ApiCallRcImpl responses = new ApiCallRcImpl();
            for (QueueItem item : itemsToReQueue)
            {
                try
                {
                    SnapshotDefinition prevSnap = item.prevSnapDfn;
                    if (prevSnap != null)
                    {
                        boolean needsNewPrevSnap = false;
                        boolean isDeleted = prevSnap.isDeleted() ||
                            prevSnap.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.DELETE);
                        if (isDeleted)
                        {
                            needsNewPrevSnap = true;
                        }
                        else
                        {
                            boolean isLastSnapOnNodeToClear = prevSnap.getAllNotDeletingSnapshots(peerAccCtx.get())
                                .size() == 1 && prevSnap.getSnapshot(peerAccCtx.get(), node.getName()) != null;
                            if (isLastSnapOnNodeToClear)
                            {
                                needsNewPrevSnap = true;
                            }
                        }
                        if (needsNewPrevSnap)
                        {
                            /*
                             * TODO: a better but far more complicated option would be to do the same flux-loop-stuff as
                             * with startQueuedL2LShippingInTransaction in order to get a new prevSnap from the
                             * targetCluster
                             * instead, we simply force a full backup
                             */
                            prevSnap = null;
                        }
                    }
                    Node shipFromNode = getNodeForBackupOrQueue(
                        item.snapDfn.getResourceDefinition(),
                        prevSnap,
                        item.snapDfn,
                        item.remote,
                        item.preferredNode,
                        responses,
                        false,
                        item.l2lData
                    );
                    if (shipFromNode != null)
                    {
                        flux = flux.concatWith(
                            startShippingInTransaction(
                                item.snapDfn,
                                shipFromNode,
                                item.remote,
                                item.prevSnapDfn,
                                responses,
                                item.preferredNode,
                                item.l2lData
                            )
                        );
                    }
                }
                catch (AccessDeniedException exc)
                {
                    responses.addEntry(exc.getMessage(), ApiConsts.MASK_ERROR);
                }
            }
            flux = flux.concatWith(Flux.just(responses));
        }
        return flux;
    }

    @Nullable
    public Node getNodeForBackupOrQueue(
        ResourceDefinition rscDfn,
        SnapshotDefinition prevSnapDfn,
        SnapshotDefinition snapDfn,
        AbsRemote remote,
        String prefNodeName,
        ApiCallRcImpl responses,
        boolean queueAnyways,
        @Nullable BackupShippingSrcData l2lData
    ) throws AccessDeniedException
    {
        Set<Node> usableNodes = backupNodeFinder.findUsableNodes(
            rscDfn,
            prevSnapDfn,
            remote
        );
        Node chosenNode = null;
        if (!queueAnyways)
        {
            chosenNode = chooseNode(usableNodes, prefNodeName, responses, remote.getType().getOptionalExtTools());
        }
        if (chosenNode == null)
        {
            // the remote needs to be the LinstorRemote in L2L-cases, since the target node is not yet decided
            // on.
            // usableNodes might be empty, in that case the snapDfn is added to the prevNodeUndecidedQueue
            backupInfoMgr.addToQueues(snapDfn, remote, prevSnapDfn, prefNodeName, l2lData, usableNodes);
        }
        return chosenNode;
    }

    private void setBackupSnapDfnFlagsAndProps(SnapshotDefinition snapDfn, String scheduleNameRef, LocalDateTime nowRef)
        throws AccessDeniedException, DatabaseException, InvalidKeyException, InvalidValueException
    {
        if (scheduleNameRef != null)
        {
            snapDfn.getSnapDfnProps(peerAccCtx.get())
                .setProp(
                    InternalApiConsts.KEY_BACKUP_SHIPPED_BY_SCHEDULE,
                    scheduleNameRef,
                    InternalApiConsts.NAMESPC_SCHEDULE
                );
        }
        /*
         * This prop ensures that upon backup restore the resource does not skip the initial sync
         * This is necessary because the metadata needs to be recreated during the restore, since uploads from different
         * nodes might have corrupted the metadata.
         * Recreating the metadata leads to the loss of the day0-uuid which is needed to skip the initial full sync
         */
        /*
         * The prop needs to be on the rscDfn during/after the restore. Any change to the way props get restored needs
         * to take this into consideration
         */
        snapDfn.getRscDfnPropsForChange(peerAccCtx.get())
            .setProp(
                InternalApiConsts.KEY_FORCE_INITIAL_SYNC_PERMA,
                ApiConsts.VAL_TRUE,
                ApiConsts.NAMESPC_DRBD_OPTIONS
            );
        snapDfn.getSnapDfnProps(peerAccCtx.get())
            .setProp(
                InternalApiConsts.KEY_BACKUP_START_TIMESTAMP,
                Long.toString(TimeUtils.getEpochMillis(nowRef)),
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );

        // save the s3 suffix as prop so that when restoring the satellite can reconstruct the .meta name
        // (s3 suffix is NOT part of snapshot name)
        String s3Suffix = sysCfgRepo.getStltConfForView(peerAccCtx.get()).getProp(
            ApiConsts.KEY_BACKUP_S3_SUFFIX,
            ApiConsts.NAMESPC_BACKUP_SHIPPING
        );
        if (s3Suffix != null)
        {
            snapDfn.getSnapDfnProps(peerAccCtx.get())
                .setProp(
                ApiConsts.KEY_BACKUP_S3_SUFFIX,
                s3Suffix,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
        }
    }

    private void setStartBackupProps(
        SnapshotDefinition snapDfn,
        String remoteName,
        List<Integer> nodeIds
    ) throws InvalidKeyException, AccessDeniedException, DatabaseException, InvalidValueException
    {
        snapDfn.getSnapDfnProps(peerAccCtx.get())
            .setProp(
                InternalApiConsts.KEY_BACKUP_NODE_IDS_TO_RESET,
                StringUtils.join(nodeIds, InternalApiConsts.KEY_BACKUP_NODE_ID_SEPERATOR),
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
        // also needed on snapDfn only for scheduled shippings
        snapDfn.getSnapDfnProps(peerAccCtx.get())
            .setProp(
                InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                remoteName,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
    }

    /**
     * Returns the number of free shipping slots - that is, the active shippings subtracted from the maximum specified
     * in KEY_MAX_CONCURRENT_BACKUPS_PER_NODE.
     * This method assumes that any new shipping that should be added based on the return value of this method does not
     * yet count as active.
     */
    public int getFreeShippingSlots(Node node) throws AccessDeniedException
    {
        int activeShippings = 0;
        for (Snapshot snap : node.getSnapshots(peerAccCtx.get()))
        {
            boolean isShipping = snap.getSnapshotDefinition().getFlags().isSet(
                peerAccCtx.get(),
                SnapshotDefinition.Flags.SHIPPING,
                SnapshotDefinition.Flags.BACKUP
            );
            /*
             * check that this is not a backup receive - we can not check if BACKUP_SOURCE is set because in l2l-cases
             * this flag gets set considerably later than the SHIPPING flag on the snapDfn
             */
            boolean isSource = snap.getFlags().isUnset(peerAccCtx.get(), Snapshot.Flags.BACKUP_TARGET);
            if (isShipping && isSource)
            {
                activeShippings++;
            }
        }
        PriorityProps prioProps = new PriorityProps(
            node.getProps(peerAccCtx.get()),
            sysCfgRepo.getCtrlConfForView(peerAccCtx.get())
        );
        String maxBackups = prioProps.getProp(
            ApiConsts.KEY_MAX_CONCURRENT_BACKUPS_PER_NODE,
            ApiConsts.NAMESPC_BACKUP_SHIPPING
        );
        int freeShippingSlots = Integer.MAX_VALUE;
        if (maxBackups != null)
        {
            int maxBackupsParsed = Integer.parseInt(maxBackups);
            freeShippingSlots = maxBackupsParsed < 0 ? Integer.MAX_VALUE : maxBackupsParsed;
        }
        return freeShippingSlots - activeShippings;
    }

    /**
     * Gets the incremental backup base for the given resource. This checks for the last successful snapshot with a
     * matching backup shipping property.
     *
     * @param rscDfn
     *     the resource definition for which previous backups should be found.
     * @param remoteName
     *     The remote name, used to memorise previous snapshots.
     * @param allowIncremental
     *     If false, this will always return null, indicating a full backup should be created.
     *
     * @return The snapshot definition of the last snapshot uploaded to the given remote. Returns null if incremental
     * backups not allowed, no snapshot was found, or the found snapshot was not compatible.
     *
     * @throws AccessDeniedException
     *     when resource definition properties can't be accessed.
     * @throws InvalidNameException
     *     when detected previous snapshot name is invalid
     */
    public SnapshotDefinition getIncrementalBase(
        ResourceDefinition rscDfn,
        AbsRemote remote,
        boolean allowIncremental,
        boolean replacePrevSnap
    )
        throws AccessDeniedException,
        InvalidNameException
    {
        SnapshotDefinition prevSnapDfn = null;
        if (allowIncremental)
        {
            String prevSnapName;
            if (replacePrevSnap)
            {
                prevSnapName = rscDfn.getProps(peerAccCtx.get())
                    .getProp(
                        InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" + remote.getName()
                    );
            }
            else
            {
                prevSnapName = rscDfn.getProps(peerAccCtx.get())
                    .getProp(
                        InternalApiConsts.KEY_BACKUP_LAST_STARTED_OR_QUEUED,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" + remote.getName()
                    );
            }

            if (prevSnapName != null)
            {
                prevSnapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscDfn, new SnapshotName(prevSnapName), false);
                if (
                    prevSnapDfn == null || prevSnapDfn.isDeleted() ||
                        prevSnapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.DELETE)
                )
                {
                    errorReporter.logWarning(
                        "Could not create an incremental backup for resource %s as the previous snapshot %s needed " +
                            "for the incremental backup has already been deleted. Creating a full backup instead.",
                        rscDfn.getName(),
                        prevSnapName
                    );
                }
                else
                {
                    if (
                        remote instanceof S3Remote &&
                            prevSnapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED)
                    )
                    {
                        S3Remote s3remote = (S3Remote) remote;
                        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                        Map<String, S3ObjectInfo> s3LinstorObjects = backupHelper.loadAllLinstorS3Objects(
                            s3remote,
                            apiCallRc
                        );
                        boolean found = false;
                        for (S3ObjectInfo s3obj : s3LinstorObjects.values())
                        {
                            SnapshotDefinition snapDfn = s3obj.getSnapDfn();
                            if (snapDfn != null && snapDfn.getUuid().equals(prevSnapDfn.getUuid()))
                            {
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                        {
                            errorReporter.logWarning(
                                "Could not create an incremental backup for resource %s as the previous backup " +
                                    "created by snapshot %s has already been deleted. Creating a full backup instead.",
                                rscDfn.getName(),
                                prevSnapName
                            );
                            // theoretically we could look for the backup before prevSnapDfn and then the one before
                            // that and so on...
                            prevSnapDfn = null;
                        }
                    }
                    if (prevSnapDfn != null)
                    {
                        for (SnapshotVolumeDefinition snapVlmDfn : prevSnapDfn.getAllSnapshotVolumeDefinitions(sysCtx))
                        {
                            long vlmDfnSize = snapVlmDfn.getVolumeDefinition().getVolumeSize(sysCtx);
                            long prevSnapVlmDfnSize = snapVlmDfn.getVolumeSize(sysCtx);
                            if (prevSnapVlmDfnSize != vlmDfnSize)
                            {
                                errorReporter.logDebug(
                                    "Current vlmDfn size (%d) does not match with prev snapDfn (%s) size (%d). " +
                                        "Forcing full backup.",
                                    vlmDfnSize,
                                    snapVlmDfn,
                                    prevSnapVlmDfnSize
                                );
                                prevSnapDfn = null;
                            }
                        }
                    }
                }
            }
            else
            {
                errorReporter.logWarning(
                    "Could not create an incremental backup for resource %s as there is no previous full backup. " +
                        "Creating a full backup instead.",
                    rscDfn.getName()
                );
            }
        }

        return prevSnapDfn;
    }

    public SnapshotDefinition getIncrementalBaseL2L(
        ResourceDefinition rscDfn,
        String prevSnapDfnUuid,
        boolean allowIncremental
    ) throws AccessDeniedException
    {
        SnapshotDefinition prevSnapDfn = null;
        if (allowIncremental)
        {
            if (prevSnapDfnUuid != null)
            {
                for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(sysCtx))
                {
                    if (snapDfn.getUuid().toString().equals(prevSnapDfnUuid))
                    {
                        prevSnapDfn = snapDfn;
                        break;
                    }
                }
            }
            if (prevSnapDfn == null)
            {
                errorReporter.logWarning(
                    "Could not create an incremental backup for resource %s as the remote cluster did not have any " +
                        "matching snapshots. Creating a full backup instead.",
                    rscDfn.getName()
                );
            }
        }
        return prevSnapDfn;
    }

    private Node chooseNode(
        Set<Node> nodesList,
        String prefNode,
        ApiCallRcImpl responses,
        Map<ExtTools, ExtToolsInfo.Version> optionalExtToolsMap
    )
        throws AccessDeniedException
    {
        List<Node> nodes = new ArrayList<>(nodesList);
        Node ret = null;
        // check prefNode first so in case pref exists, it is not checked twice
        Node pref = prefNode == null ? null : ctrlApiDataLoader.loadNode(prefNode, false);
        if (pref != null && nodes.contains(pref) && getFreeShippingSlots(pref) > 0)
        {
            ret = pref;
        }
        else
        {
            TreeMap<Integer, List<Node>> sortedWithExtTools = new TreeMap<>();
            TreeMap<Integer, List<Node>> sortedNoExtTools = new TreeMap<>();

            for (Node node : nodes)
            {
                int freeShippingSlots = getFreeShippingSlots(node);
                if (freeShippingSlots > 0)
                {
                    Map<Integer, List<Node>> targetMap;
                    if (hasNodeAllExtTools(node, optionalExtToolsMap, null, null, peerAccCtx.get()))
                    {
                        targetMap = sortedWithExtTools;
                    }
                    else
                    {
                        targetMap = sortedNoExtTools;
                    }

                    targetMap.computeIfAbsent(
                        freeShippingSlots,
                        k -> new ArrayList<>()
                    ).add(node);
                }
                // else no slots open
            }
            // take the one with the most free shipping slots, preferably from the list with all ext tools
            if (!sortedWithExtTools.isEmpty())
            {
                ret = sortedWithExtTools.lastEntry().getValue().get(0);
            }
            else if (!sortedNoExtTools.isEmpty())
            {
                ret = sortedNoExtTools.lastEntry().getValue().get(0);
            }
            if (ret != null)
            {
                if (prefNode != null)
                {
                    responses.addEntry(
                        "Preferred node '" + prefNode + "' could not be selected. Choosing '" + ret.getName() +
                            "' instead.",
                        ApiConsts.MASK_WARN
                    );
                }
            }
            else
            {
                responses.addEntry(
                    "Maximum amount of shippings met on all nodes, adding to queue instead",
                    ApiConsts.MASK_WARN
                );
            }
        }
        return ret;
    }

    /**
     * Makes sure the given node has all ext-tools given
     */
    public static boolean hasNodeAllExtTools(
        Node node,
        Map<ExtTools, ExtToolsInfo.Version> extTools,
        ApiCallRcImpl apiCallRcRef,
        String errorMsgPrefix,
        AccessContext accCtx
    )
        throws AccessDeniedException
    {
        boolean ret = true;
        if (extTools != null)
        {
            ExtToolsManager extToolsMgr = node.getPeer(accCtx).getExtToolsManager();
            StringBuilder sb = new StringBuilder();
            for (Entry<ExtTools, Version> extTool : extTools.entrySet())
            {
                ExtToolsInfo extToolInfo = extToolsMgr.getExtToolInfo(extTool.getKey());
                Version requiredVersion = extTool.getValue();
                if (
                    extToolInfo == null || !extToolInfo.isSupported() ||
                        (requiredVersion != null && !extToolInfo.hasVersionOrHigher(requiredVersion))
                )
                {
                    ret = false;
                    sb.append(extTool.getKey());
                    if (requiredVersion != null)
                    {
                        sb.append(" (").append(requiredVersion.toString()).append(")");
                    }
                    sb.append(", ");
                }
            }
            if (sb.length() > 0 && apiCallRcRef != null)
            {
                sb.setLength(sb.length() - 2);
                apiCallRcRef.addEntry(errorMsgPrefix + sb.toString(), ApiConsts.MASK_INFO);
            }
        }
        return ret;
    }

    /**
     * Sets all the props that differ depending on whether the backup is full or incremental
     */
    void setIncrementalDependentProps(
        SnapshotDefinition curSnapDfn,
        SnapshotDefinition prevSnapDfn,
        String remoteName,
        String scheduleName
    )
        throws InvalidValueException, AccessDeniedException, DatabaseException
    {
        Props snapDfnProps = curSnapDfn.getSnapDfnProps(peerAccCtx.get());
        Props rscDfnProps = curSnapDfn.getResourceDefinition().getProps(peerAccCtx.get());
        if (prevSnapDfn == null)
        {
            snapDfnProps.setProp(
                InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP,
                curSnapDfn.getName().displayValue,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
            if (scheduleName != null)
            {
                rscDfnProps.setProp(
                    remoteName + ReadOnlyProps.PATH_SEPARATOR + scheduleName + ReadOnlyProps.PATH_SEPARATOR +
                        InternalApiConsts.KEY_LAST_BACKUP_INC,
                    ApiConsts.VAL_FALSE,
                    InternalApiConsts.NAMESPC_SCHEDULE
                );
            }
        }
        else
        {
            snapDfnProps.setProp(
                InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP,
                prevSnapDfn.getSnapDfnProps(peerAccCtx.get())
                    .getProp(
                        InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    ),
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
            if (scheduleName != null)
            {
                rscDfnProps.setProp(
                    remoteName + ReadOnlyProps.PATH_SEPARATOR + scheduleName + ReadOnlyProps.PATH_SEPARATOR +
                        InternalApiConsts.KEY_LAST_BACKUP_INC,
                    ApiConsts.VAL_TRUE,
                    InternalApiConsts.NAMESPC_SCHEDULE
                );
            }
        }
        if (scheduleName != null)
        {
            rscDfnProps.setProp(
                remoteName + ReadOnlyProps.PATH_SEPARATOR + scheduleName + ReadOnlyProps.PATH_SEPARATOR +
                    InternalApiConsts.KEY_LAST_BACKUP_TIME,
                Long.toString(System.currentTimeMillis()),
                InternalApiConsts.NAMESPC_SCHEDULE
            );
        }
    }
}
