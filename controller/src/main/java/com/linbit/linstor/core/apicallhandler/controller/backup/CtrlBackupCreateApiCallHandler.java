package com.linbit.linstor.core.apicallhandler.controller.backup;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
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
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotShippingAbortHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupApiHelper.S3ObjectInfo;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler.BackupShippingData;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler.BackupShippingRestClient;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingRequestPrevSnap;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingResponsePrevSnap;
import com.linbit.linstor.core.apicallhandler.controller.backup.nodefinder.BackupNodeFinder;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.req.CreateMultiSnapRequest;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Schedule;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.core.repository.SystemConfProtectionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
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
import com.linbit.utils.ExceptionThrowingIterator;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

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
    private final Provider<Peer> peerProvider;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlSnapshotShippingAbortHandler ctrlSnapShipAbortHandler;
    private final RemoteRepository remoteRepo;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlBackupApiHelper backupHelper;
    private final CtrlScheduledBackupsApiCallHandler scheduledBackupsHandler;
    private final BackupNodeFinder backupNodeFinder;
    private final NodeRepository nodeRepo;
    private final BackupShippingRestClient restClient;
    private final Provider<CtrlBackupL2LSrcApiCallHandler> backupL2LSrcHandler;

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
        Provider<Peer> peerProviderRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlSnapshotShippingAbortHandler ctrlSnapShipAbortHandlerRef,
        RemoteRepository remoteRepoRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlBackupApiHelper backupHelperRef,
        CtrlScheduledBackupsApiCallHandler scheduledBackupsHandlerRef,
        BackupNodeFinder backupNodeFinderRef,
        NodeRepository nodeRepoRef,
        BackupShippingRestClient restClientRef,
        Provider<CtrlBackupL2LSrcApiCallHandler> backupL2LSrcHandlerRef
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
        peerProvider = peerProviderRef;
        backupInfoMgr = backupInfoMgrRef;
        ctrlSnapShipAbortHandler = ctrlSnapShipAbortHandlerRef;
        remoteRepo = remoteRepoRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        backupHelper = backupHelperRef;
        scheduledBackupsHandler = scheduledBackupsHandlerRef;
        backupNodeFinder = backupNodeFinderRef;
        nodeRepo = nodeRepoRef;
        restClient = restClientRef;
        backupL2LSrcHandler = backupL2LSrcHandlerRef;

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
                new Date(),
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
        Date nowRef,
        boolean allowIncremental,
        RemoteType remoteTypeRef,
        String scheduleNameRef,
        boolean runInBackgroundRef,
        @Nullable String prevSnapDfnUuid,
        @Nullable BackupShippingData l2lData
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
            AbsRemote remote = null;
            SnapshotDefinition prevSnapDfn = null;
            if (remoteTypeRef.equals(RemoteType.S3))
            {
                // check if encryption is possible
                backupHelper.getLocalMasterKey();

                remote = backupHelper.getRemote(remoteName);
                prevSnapDfn = getIncrementalBase(rscDfn, remote, allowIncremental, false);
            }
            else if (remoteTypeRef.equals(RemoteType.LINSTOR))
            {
                remote = backupHelper.getRemote(remoteName);
                if (remote instanceof LinstorRemote)
                {
                    prevSnapDfn = getIncrementalBaseL2L(rscDfn, prevSnapDfnUuid, allowIncremental);
                }
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
                    startShipping(snapDfn, chosenNode, remote, prevSnapDfn, responses)
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
        ApiCallRcImpl responses
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Start backup shipping",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> startShippingInTransaction(snapDfn, node, remote, prevSnapDfn, responses)
        );
    }

    Flux<ApiCallRc> startShippingInTransaction(
        SnapshotDefinition snapDfn,
        Node node,
        AbsRemote remote,
        SnapshotDefinition prevSnapDfn,
        ApiCallRcImpl responsesRef
    )
    {
        try
        {
            snapDfn.getFlags()
                .enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING, SnapshotDefinition.Flags.BACKUP);
            if (remote instanceof S3Remote)
            {
                snapDfn.getProps(peerAccCtx.get())
                    .setProp(
                        InternalApiConsts.KEY_BACKUP_SRC_NODE + "/" + remote.getName(),
                        node.getName().displayValue,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );
            }
            else if (remote instanceof LinstorRemote)
            {
                snapDfn.getProps(peerAccCtx.get())
                    .setProp(
                        InternalApiConsts.KEY_BACKUP_SRC_NODE + "/" + remote.getName() +
                            "/" + snapDfn.getResourceName().displayValue,
                        node.getName().displayValue,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );
            }
            // now that it is decided that this node will do the shipping, see if any snapDfn can be moved to the normal
            // queues
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
                snap.getProps(peerAccCtx.get())
                    .setProp(
                        InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                        remote.getName().displayValue,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );
            }
            if (prevSnapDfn != null)
            {
                snap.getProps(peerAccCtx.get())
                    .setProp(
                        InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                        prevSnapDfn.getName().displayValue,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );
            }

            ctrlTransactionHelper.commit();
            return ctrlSatelliteUpdateCaller.updateSatellites(snapDfn, CtrlSatelliteUpdateCaller.notConnectedWarn())
                .transform(
                    responses -> CtrlResponseUtils
                        .combineResponses(responses, snapDfn.getResourceName(), "Started shipping of resource {1}")
                )
                .concatWith(
                    snapshotCrtHandler.removeInProgressSnapshots(new CreateMultiSnapRequest(peerAccCtx.get(), snapDfn))
                );
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
                flux.subscriberContext(
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
        List<QueueItem> itemsToReQueue = backupInfoMgr.deleteFromQueue(node);
        ApiCallRcImpl responses = new ApiCallRcImpl();
        Flux<ApiCallRc> flux = Flux.empty();
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
                            responses
                        )
                    );
                }
            }
            catch (AccessDeniedException exc)
            {
                responses.addEntry(exc.getMessage(), ApiConsts.MASK_ERROR);
            }
        }
        return flux.concatWith(Flux.just(responses));
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
            flux = deleteNodeQueueAndReQueueSnapsIfNeeded(node);
        }
        else
        {
            if (getFreeShippingSlots(node) > 0)
            {
                flux = startMultipleQueuedShippings(
                    node,
                    new IteratorFromNodeQueue(node)
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
        Collection<Node> nodes = nodeRepo.getMapForView(peerAccCtx.get()).values();
        List<Node> nodesToClear = new ArrayList<>();
        List<Node> nodesToStart = new ArrayList<>();
        int toClearCt = 0;
        for (Node node : nodes)
        {
            Props nodeProps = node.getProps(peerAccCtx.get());
            PriorityProps prioProps = new PriorityProps(
                nodeProps,
                sysCfgRepo.getCtrlConfForView(peerAccCtx.get())
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
                flux = flux.concatWith(deleteNodeQueueAndReQueueSnapsIfNeeded(node));
            }
            for (Node node : nodesToStart)
            {
                /*
                 * This is just a pre-filter; since the result of getFreeShippingSlots() changes due to the results of
                 * the following flux(es), we cannot be certain that the result of this if is still valid when we get to
                 * actually trying to start shipments, which is why there are additional checks for
                 * getFreeShippingSlots() later on
                 */
                if (getFreeShippingSlots(node) > 0)
                {
                    flux = flux.concatWith(
                        startMultipleQueuedShippings(
                            node,
                            new IteratorFromNodeQueue(node)
                        )
                    );
                }
            }
        }
        return flux;
    }

    private @Nullable Node getNodeForBackupOrQueue(
        ResourceDefinition rscDfn,
        SnapshotDefinition prevSnapDfn,
        SnapshotDefinition snapDfn,
        AbsRemote remote,
        String prefNodeName,
        ApiCallRcImpl responses,
        boolean queueAnyways,
        @Nullable BackupShippingData l2lData
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

    private void setBackupSnapDfnFlagsAndProps(SnapshotDefinition snapDfn, String scheduleNameRef, Date nowRef)
        throws AccessDeniedException, DatabaseException, InvalidKeyException, InvalidValueException
    {
        if (scheduleNameRef != null)
        {
            snapDfn.getProps(peerAccCtx.get()).setProp(
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
        snapDfn.getProps(peerAccCtx.get())
            .setProp(
                InternalApiConsts.KEY_FORCE_INITIAL_SYNC_PERMA,
                ApiConsts.VAL_TRUE,
                ApiConsts.NAMESPC_DRBD_OPTIONS
            );
        snapDfn.getProps(peerAccCtx.get())
            .setProp(
                InternalApiConsts.KEY_BACKUP_START_TIMESTAMP,
                Long.toString(nowRef.getTime()),
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
            snapDfn.getProps(peerAccCtx.get()).setProp(
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
        snapDfn.getProps(peerAccCtx.get()).setProp(
            InternalApiConsts.KEY_BACKUP_NODE_IDS_TO_RESET,
            StringUtils.join(nodeIds, InternalApiConsts.KEY_BACKUP_NODE_ID_SEPERATOR),
            ApiConsts.NAMESPC_BACKUP_SHIPPING
        );
        // also needed on snapDfn only for scheduled shippings
        snapDfn.getProps(peerAccCtx.get())
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
    private int getFreeShippingSlots(Node node) throws AccessDeniedException
    {
        int activeShippings = 0;
        for (Snapshot snap : node.getSnapshots(peerAccCtx.get()))
        {
            boolean isShipping = snap.getSnapshotDefinition().getFlags().isSet(
                peerAccCtx.get(),
                SnapshotDefinition.Flags.SHIPPING,
                SnapshotDefinition.Flags.BACKUP
            );
            boolean isSource = snap.getFlags().isSet(peerAccCtx.get(), Snapshot.Flags.BACKUP_SOURCE);
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
    private SnapshotDefinition getIncrementalBase(
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

    private SnapshotDefinition getIncrementalBaseL2L(
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
        Props snapDfnProps = curSnapDfn.getProps(peerAccCtx.get());
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
                    remoteName + Props.PATH_SEPARATOR + scheduleName + Props.PATH_SEPARATOR +
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
                prevSnapDfn.getProps(peerAccCtx.get()).getProp(
                    InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                ),
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
            if (scheduleName != null)
            {
                rscDfnProps.setProp(
                    remoteName + Props.PATH_SEPARATOR + scheduleName + Props.PATH_SEPARATOR +
                        InternalApiConsts.KEY_LAST_BACKUP_INC,
                    ApiConsts.VAL_TRUE,
                    InternalApiConsts.NAMESPC_SCHEDULE
                );
            }
        }
        if (scheduleName != null)
        {
            rscDfnProps.setProp(
                remoteName + Props.PATH_SEPARATOR + scheduleName + Props.PATH_SEPARATOR +
                    InternalApiConsts.KEY_LAST_BACKUP_TIME,
                Long.toString(System.currentTimeMillis()),
                InternalApiConsts.NAMESPC_SCHEDULE
            );
        }
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
                    .write(LockObj.RSC_DFN_MAP).buildDeferred(),
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
            "Backup shipping for snapshot %s of resource %s %s", snapNameRef, rscNameRef,
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
                        .abortBackupShippingPrivileged(snapDfn)
                        .concatWith(
                            backupHelper.startStltCleanup(
                                peerProvider.get(), rscNameRef, snapNameRef, peerProvider.get().getNode().getName()
                            )
                        );
                    Snapshot snap = snapDfn.getSnapshot(peerAccCtx.get(), nodeName);
                    if (snap != null && !snap.isDeleted())
                    {
                        // no idea how snap could be null or deleted here, but keep check just in case
                        String remoteName = snap.getProps(peerAccCtx.get()).removeProp(
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
                        String remoteName = snap.getProps(peerAccCtx.get()).removeProp(
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
                                        snapNameRef,
                                        peerProvider.get().getNode().getName()
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
                            snapDfn, nodeName, rscDfn, schedule, remoteForSchedule, successRef, forceSkip
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
                            rscDfn.getName().displayValue, scheduleName
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
                        if (getFreeShippingSlots(currentNode) > 0)
                        {
                            cleanupFlux = cleanupFlux.concatWith(startFollowUpShippings(queueItem, currentNode));
                        }
                    }
                }
                // If the previous loop didn't fill all shipping slots of this node, start more shipments here
                Node node = peerProvider.get().getNode();
                if (getFreeShippingSlots(node) > 0)
                {
                    cleanupFlux = cleanupFlux.concatWith(
                        startMultipleQueuedShippings(
                            node,
                            new IteratorFromNodeQueue(node)
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
    private Flux<ApiCallRc> startMultipleQueuedShippings(
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
            if (getFreeShippingSlots(node) > 0 && nextItem.hasNext())
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
            if (getFreeShippingSlots(node) > 0)
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
                                prevSnapDfn = getIncrementalBase(
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
                                    nodeForShipping = getNodeForBackupOrQueue(
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
                                        startShippingInTransaction(
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
                SnapshotDefinition l2lPrevSnapDfn = getIncrementalBaseL2L(
                    next.snapDfn.getResourceDefinition(),
                    resp.prevSnapUuid,
                    next.prevSnapDfn != null
                );
                boolean queueAnyways = l2lPrevSnapDfn != null && l2lPrevSnapDfn.getFlags()
                    .isUnset(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED);
                l2lNodeForShipping = getNodeForBackupOrQueue(
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
                    ret = startShippingInTransaction(
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
                    ret = Flux.just(resp.responses);
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
                    rscDfn.getProps(peerAccCtx.get()).setProp(
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
                    rscDfn.getProps(peerAccCtx.get()).setProp(
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

    private class IteratorFromNodeQueue implements ExceptionThrowingIterator<QueueItem, AccessDeniedException>
    {
        private Node node;

        IteratorFromNodeQueue(Node nodeRef)
        {
            node = nodeRef;

        }

        @Override
        public boolean hasNext()
        {
            return backupInfoMgr.hasNodeQueuedSnaps(node);
        }

        @Override
        public QueueItem next() throws AccessDeniedException
        {
            return backupInfoMgr.getNextFromQueue(peerAccCtx.get(), node);
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
