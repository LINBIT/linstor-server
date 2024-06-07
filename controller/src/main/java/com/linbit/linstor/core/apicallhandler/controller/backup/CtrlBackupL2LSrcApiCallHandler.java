package com.linbit.linstor.core.apicallhandler.controller.backup;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.BackupInfoManager.CleanupData;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingRestClient;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingReceiveRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingRequestPrevSnap;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingResponse;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingResponsePrevSnap;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingSrcData;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlBackupQueueInternalCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.core.objects.remotes.StltRemoteControllerFactory;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsLayerHelperUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.tasks.StltRemoteCleanupTask;
import com.linbit.linstor.tasks.TaskScheduleService;
import com.linbit.linstor.tasks.TaskScheduleService.Task;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.Pair;

import static com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller.notConnectedError;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import reactor.core.publisher.Flux;

/**
 * Creates and ships a backup to a linstor cluster, using the following steps:</br>
 * 1) find all snapshots that can function as a base snap for an incremental shipping and send their uuids to the target
 * cluster</br>
 * ~~~ continue when target cluster responds with the chosen base snap uuid ~~~</br>
 * 2) create the snapshot and use the base snap the target cluster decided on to find out which node(s) can do the
 * shipping </br>
 * 2a) if there is no node available to do the shipping, queue it instead and delay the following steps until it is
 * processed from the queue</br>
 * 3) create the stlt-remote and tell the target cluster to start receiving</br>
 * ~~~ continue when target cluster responds with the port it is listening on ~~~</br>
 * 4) start sending the backup to the target cluster
 */
@Singleton
public class CtrlBackupL2LSrcApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext sysCtx;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final StltRemoteControllerFactory stltRemoteFactory;

    private final BackupShippingRestClient restClient;
    private final CtrlBackupCreateApiCallHandler ctrlBackupCrtApiCallHandler;
    private final SystemConfRepository systemConfRepository;
    private final BackupInfoManager backupInfoMgr;
    private final RemoteRepository remoteRepo;
    private final CtrlSecurityObjects ctrlSecObjs;
    private final CtrlBackupQueueInternalCallHandler queueHandler;
    private final TaskScheduleService taskScheduleService;

    @Inject
    public CtrlBackupL2LSrcApiCallHandler(
        @SystemContext AccessContext sysCtxRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        StltRemoteControllerFactory stltRemoteFactoryRef,
        CtrlBackupCreateApiCallHandler ctrlBackupCrtApiCallHandlerRef,
        SystemConfRepository systemConfRepositoryRef,
        RemoteRepository remoteRepoRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlSecurityObjects ctrlSecObjsRef,
        BackupShippingRestClient restClientRef,
        CtrlBackupQueueInternalCallHandler queueHandlerRef,
        TaskScheduleService taskScheduleServiceRef,
        ErrorReporter errorReporterRef
    )
    {
        sysCtx = sysCtxRef;
        peerAccCtx = peerAccCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        stltRemoteFactory = stltRemoteFactoryRef;
        ctrlBackupCrtApiCallHandler = ctrlBackupCrtApiCallHandlerRef;
        systemConfRepository = systemConfRepositoryRef;
        remoteRepo = remoteRepoRef;
        backupInfoMgr = backupInfoMgrRef;
        ctrlSecObjs = ctrlSecObjsRef;

        restClient = restClientRef;
        queueHandler = queueHandlerRef;
        taskScheduleService = taskScheduleServiceRef;
        errorReporter = errorReporterRef;
    }

    /**
     * (see class-javadoc for overview)</br>
     * 1) find all snapshots that can function as a base snap for an incremental shipping and send their uuids to the
     * target cluster</br>
     * also do a bunch of checks to ensure the shipping is possible in the first place
     */
    public Flux<ApiCallRc> shipBackup(
        String srcNodeNameRef,
        String srcRscNameRef,
        String linstorRemoteNameRef,
        String dstRscNameRef,
        @Nullable String dstNodeNameRef,
        @Nullable String dstNetIfNameRef,
        @Nullable String dstStorPoolRef,
        @Nullable Map<String, String> storPoolRenameRef,
        boolean downloadOnly,
        boolean forceRestore,
        String scheduleNameRef,
        boolean allowIncremental,
        boolean runInBackgroundRef
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Backup shipping L2L: Get base snapshot",
            lockGuardFactory.create()
                .write(LockObj.REMOTE_MAP)
                .buildDeferred(),
            () -> shipBackupInTransaction(
                srcNodeNameRef,
                srcRscNameRef,
                linstorRemoteNameRef,
                dstRscNameRef,
                dstNodeNameRef,
                dstNetIfNameRef,
                dstStorPoolRef,
                storPoolRenameRef,
                downloadOnly,
                forceRestore,
                scheduleNameRef,
                allowIncremental,
                runInBackgroundRef
            )
        );
    }

    private Flux<ApiCallRc> shipBackupInTransaction(
        String srcNodeNameRef,
        String srcRscNameRef,
        String linstorRemoteNameRef,
        String dstRscNameRef,
        String dstNodeNameRef,
        String dstNetIfNameRef,
        String dstStorPoolRef,
        Map<String, String> storPoolRenameRef,
        boolean downloadOnly,
        boolean forceRestore,
        String scheduleNameRef,
        boolean allowIncremental,
        boolean runInBackgroundRef
    )
    {
        AbsRemote remote = ctrlApiDataLoader.loadRemote(linstorRemoteNameRef, true);

        if (!(remote instanceof LinstorRemote))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REMOTE_NAME,
                    "The given remote is not a linstor-remote",
                    true
                )
            );
        }
        String srcClusterId;
        try
        {
            srcClusterId = systemConfRepository.getCtrlConfForView(sysCtx).getProp(
                InternalApiConsts.KEY_CLUSTER_LOCAL_ID,
                ApiConsts.NAMESPC_CLUSTER
            );
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        // build list of possible base snapshots for incremental shipping
        // TODO: sort into two sets - one "snap to ship + older", other "newer than snap to ship" - second group will be
        // empty for now, preparation for shipping specific snap
        Set<String> srcSnapDfnUuids = new HashSet<>();
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(srcRscNameRef, true);
        try
        {
            for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(sysCtx))
            {
                if (!snapDfn.getAllSnapshots(sysCtx).isEmpty())
                {
                    srcSnapDfnUuids.add(snapDfn.getUuid().toString());
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "Finding possible base snapshot definitions",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        Date now = new Date();
        String backupName = BackupShippingUtils.generateBackupName(now);
        Map<String, String> storPoolRenameMap = new HashMap<>();
        if (storPoolRenameRef != null)
        {
            storPoolRenameMap.putAll(storPoolRenameRef);
        }
        if (dstStorPoolRef != null)
        {
            storPoolRenameMap.put(AbsLayerHelperUtils.RENAME_STOR_POOL_DFLT_KEY, dstStorPoolRef);
        }
        BackupShippingSrcData data = new BackupShippingSrcData(
            srcClusterId,
            srcNodeNameRef,
            srcRscNameRef,
            backupName,
            now,
            (LinstorRemote) remote,
            dstRscNameRef,
            dstNodeNameRef,
            dstNetIfNameRef,
            dstStorPoolRef,
            storPoolRenameMap,
            downloadOnly,
            forceRestore,
            scheduleNameRef,
            allowIncremental
        );

        return Flux.merge(
            restClient.sendPrevSnapRequest(
                new BackupShippingRequestPrevSnap(
                    LinStor.VERSION_INFO_PROVIDER.getSemanticVersion(),
                    srcClusterId,
                    dstRscNameRef,
                    srcSnapDfnUuids,
                    dstNodeNameRef
                ),
                (LinstorRemote) remote,
                sysCtx
            )
                .map(
                    resp -> scopeRunner.fluxInTransactionalScope(
                        "Backup shipping L2L: Create Snapshots",
                        lockGuardFactory.create()
                            .read(LockObj.NODES_MAP)
                            .write(LockObj.RSC_DFN_MAP)
                            .buildDeferred(),
                        () -> createSnapshot(resp, data, runInBackgroundRef)
                    )
                )
        );
    }

    /**
     * (see class-javadoc for overview)</br>
     * 2) create the snapshot and use the base snap the target cluster decided on to find out which node(s) can do the
     * shipping </br>
     * 2a) if there is no node available to do the shipping, queue it instead and delay the following steps until it is
     * processed from the queue
     */
    private Flux<ApiCallRc> createSnapshot(
        BackupShippingResponsePrevSnap response,
        BackupShippingSrcData data,
        boolean runInBackgroundRef
    )
    {
        Flux<ApiCallRc> flux;
        if (!response.canReceive)
        {
            flux = Flux.just(response.responses);
        }
        else
        {
            Map<ExtTools, Version> requiredExtTools = new HashMap<>();
            requiredExtTools.put(ExtTools.SOCAT, null);
            Map<ExtTools, Version> optionalExtTools = new HashMap<>();
            optionalExtTools.put(ExtTools.ZSTD, null);
            data.setResetData(response.resetData);
            data.setDstBaseSnapName(response.dstBaseSnapName);
            data.setDstActualNodeName(response.dstActualNodeName);
            Pair<Flux<ApiCallRc>, Snapshot> createSnapshot = ctrlBackupCrtApiCallHandler.backupSnapshot(
                data.getSrcRscName(),
                data.getLinstorRemote().getName().displayValue,
                data.getSrcNodeName(),
                data.getSrcBackupName(),
                data.getNow(),
                data.isAllowIncremental() && response.prevSnapUuid != null,
                RemoteType.LINSTOR,
                data.getScheduleName(),
                runInBackgroundRef,
                response.prevSnapUuid,
                data
            );
            if (createSnapshot.objB != null)
            {
                data.setSrcSnapshot(createSnapshot.objB);
                data.setSrcNodeName(data.getSrcSnapshot().getNode().getName().displayValue);
                flux = createSnapshot.objA
                    .concatWith(
                        scopeRunner.fluxInTransactionalScope(
                            "Backup shipping L2L: Create Stlt-Remote",
                            lockGuardFactory.create()
                                .read(LockObj.NODES_MAP)
                                .write(LockObj.RSC_DFN_MAP)
                                .buildDeferred(),
                            () -> createStltRemoteInTransaction(data, createSnapshot.objB.getNode())
                        )
                    );
            }
            else
            {
                flux = createSnapshot.objA;
            }
        }
        return flux;
    }

    /**
     * (see class-javadoc for overview)</br>
     * 3) create the stlt-remote</br>
     * also calls updateSatellites
     */
    public Flux<ApiCallRc> createStltRemoteInTransaction(BackupShippingSrcData data, Node node)
    {
        StltRemote stltRemote = createStltRemote(
            stltRemoteFactory,
            remoteRepo,
            peerAccCtx.get(),
            data.getSrcRscName(),
            data.getSrcBackupName(),
            new TreeMap<>(),
            data.getLinstorRemote().getName(),
            node,
            data.getDstRscName()
        );

        data.setStltRemote(stltRemote);

        ctrlTransactionHelper.commit();
        return ctrlSatelliteUpdateCaller.updateSatellites(stltRemote)
            .concatWith(
                scopeRunner.fluxInTransactionalScope(
                    "Backup shipping L2L: Sending backup request to destination controller",
                    lockGuardFactory.create()
                        .read(LockObj.NODES_MAP)
                        .write(LockObj.RSC_DFN_MAP)
                        .buildDeferred(),
                    () -> prepareShippingInTransaction(data)
                )
            );
    }

    /*
     * Method also used by CtrlBackupL2LDstApiCallHandler
     */
    static StltRemote createStltRemote(
        StltRemoteControllerFactory stltRemoteFactoryRef,
        RemoteRepository remoteRepoRef,
        AccessContext accCtxRef,
        String rscNameRef,
        String snapshotNameRef,
        Map<String, Integer> snapShipPortsRef,
        RemoteName linstorRemoteNameRef,
        Node nodeRef,
        String otherRscNameRef
    )
    {
        StltRemote stltRemote;
        try
        {
            stltRemote = stltRemoteFactoryRef.create(
                accCtxRef,
                // add random uuid to avoid naming conflict
                RemoteName.createStltRemoteName(rscNameRef, snapshotNameRef, UUID.randomUUID()),
                null,
                snapShipPortsRef,
                linstorRemoteNameRef,
                nodeRef,
                otherRscNameRef
            );
            remoteRepoRef.put(accCtxRef, stltRemote);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "Setting backup source flag to snapshot",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        return stltRemote;
    }

    /**
     * (see class-javadoc for overview)</br>
     * 3) create the stlt-remote and tell the target cluster to start receiving
     */
    private Flux<ApiCallRc> prepareShippingInTransaction(BackupShippingSrcData data)
    {
        Flux<ApiCallRc> flux;
        try
        {
            BackupMetaDataPojo metaDataPojo = BackupShippingUtils.getBackupMetaDataPojo(
                peerAccCtx.get(),
                data.getSrcSnapshot(),
                systemConfRepository.getStltConfForView(sysCtx),
                ctrlSecObjs.getEncKey(),
                ctrlSecObjs.getCryptHash(),
                ctrlSecObjs.getCryptSalt(),
                Collections.emptyMap(),
                null
            );

            NodeName srcSendingNodeName = data.getSrcSnapshot().getNodeName();
            backupInfoMgr.abortCreateAddL2LEntry(
                srcSendingNodeName,
                data.getSrcSnapshot().getSnapshotDefinition().getSnapDfnKey(),
                data.getStltRemote().getLinstorRemoteName()
            );

            ExtToolsManager extToolsMgr = data.getSrcSnapshot().getNode().getPeer(sysCtx).getExtToolsManager();
            ExtToolsInfo zstd = extToolsMgr.getExtToolInfo(ExtTools.ZSTD);
            data.setUseZstd(zstd != null && zstd.isSupported());

            data.setMetaDataPojo(metaDataPojo);
            data.getMetaDataPojo()
                .getRscDfn()
                .getProps()
                .put(
                InternalApiConsts.KEY_BACKUP_L2L_SRC_SNAP_DFN_UUID,
                    data.getSrcSnapshot().getSnapshotDefinition().getUuid().toString()
            );
            // tell target cluster "Hey! Listen!"
            flux = Flux.merge(
                restClient.sendBackupRequest(data, peerAccCtx.get())
                    .map(
                        restResponse -> scopeRunner.fluxInTransactionalScope(
                            "Backup shipping L2L: Starting shipment",
                            lockGuardFactory.create()
                                .read(LockObj.NODES_MAP)
                                .write(LockObj.RSC_DFN_MAP)
                                .buildDeferred(),
                            () -> confirmBackupShippingRequestArrived(restResponse, data)
                        )
                    )
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "preparing backup shipping", ApiConsts.FAIL_ACC_DENIED_RSC_DFN);
        }
        catch (ParseException exc)
        {
            throw new ImplementationError("Invalid backup name generated", exc);
        }
        return flux;
    }

    private Flux<ApiCallRc> confirmBackupShippingRequestArrived(
        BackupShippingResponse responseRef,
        BackupShippingSrcData data
    )
    {
        return Flux.just(
            ApiCallRcImpl.copyAndPrefix(
                "Remote '" + data.getLinstorRemote().getName().displayValue + "': ",
                responseRef.responses
            )
        );
    }

    /**
     * (see class-javadoc for overview)</br>
     * 4) start sending the backup to the target cluster
     * Updates stltRemote with received IP/Port and starts shipment
     */
    public Flux<ApiCallRc> startShipping(
        BackupShippingReceiveRequest responseRef,
        BackupShippingSrcData data
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Backup shipping L2L: make stlt start shipping",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP).buildDeferred(),
            () -> startShippingInTransaction(responseRef, data)
        );
    }

    private Flux<ApiCallRc> startShippingInTransaction(
        BackupShippingReceiveRequest responseRef,
        BackupShippingSrcData data
    )
    {
        Flux<ApiCallRc> flux;
        try
        {
            if (responseRef.canReceive)
            {
                AccessContext accCtx = peerAccCtx.get();
                StltRemote stltRemote = data.getStltRemote();
                stltRemote.setIp(accCtx, responseRef.dstStltIp);
                stltRemote.setAllPorts(accCtx, responseRef.dstStltPorts);
                stltRemote.useZstd(accCtx, responseRef.useZstd && data.isUseZstd());
                ctrlTransactionHelper.commit();
                flux = ctrlSatelliteUpdateCaller.updateSatellites(stltRemote);

                Snapshot snap = data.getSrcSnapshot();
                snap.getFlags().enableFlags(accCtx, Snapshot.Flags.BACKUP_SOURCE);
                snap.setTakeSnapshot(accCtx, true); // needed by source-satellite to actually start sending
                SnapshotDefinition prevSnapDfn = null;
                if (
                    responseRef.srcSnapDfnUuid != null && !responseRef.srcSnapDfnUuid.isEmpty() && data
                        .isAllowIncremental()
                )
                {
                    UUID prevSnapUuid = UUID.fromString(responseRef.srcSnapDfnUuid);
                    for (SnapshotDefinition snapDfn : snap.getResourceDefinition().getSnapshotDfns(accCtx))
                    {
                        if (snapDfn.getUuid().equals(prevSnapUuid))
                        {
                            prevSnapDfn = snapDfn;
                            break;
                        }
                    }
                    if (prevSnapDfn == null)
                    {
                        throw new ImplementationError(
                            "SnapshotDefinition selected by destination cluster not found locally: " +
                                responseRef.srcSnapDfnUuid
                        );
                    }

                }
                backupInfoMgr.addCleanupData(data);
                ctrlBackupCrtApiCallHandler.setIncrementalDependentProps(
                    snap.getSnapshotDefinition(),
                    prevSnapDfn,
                    data.getLinstorRemote().getName().displayValue,
                    data.getScheduleName()
                );
                snap.getProps(peerAccCtx.get())
                    .setProp(
                        InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                        stltRemote.getName().displayValue,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );
                ctrlTransactionHelper.commit();
                flux = flux.concatWith(
                    ctrlSatelliteUpdateCaller.updateSatellites(snap.getSnapshotDefinition(), notConnectedError())
                        .transform(
                            updateResponses -> CtrlResponseUtils.combineResponses(
                                errorReporter,
                                updateResponses,
                                snap.getResourceName(),
                                "Starting shipment of {1} on {0} "
                            )
                        )
                )
                    .concatWith(
                        scopeRunner.fluxInTransactionalScope(
                            "Backup shipping L2L: Cleanup after start",
                            lockGuardFactory.create()
                                .read(LockObj.NODES_MAP)
                                .write(LockObj.RSC_DFN_MAP).buildDeferred(),
                            () -> unsetTakeSnapshotInTransaction(data)
                        )
                    );
            }
            else
            {
                flux = Flux.just(
                    ApiCallRcImpl.copyAndPrefix(
                        "Remote '" + data.getLinstorRemote().getName().displayValue + "': ",
                        responseRef.responses
                    )
                );
            }

        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "Setting backup source flag to snapshot",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        return flux;
    }

    public Flux<ApiCallRc> startQueueIfReady(StltRemote stltRemote, boolean allowNullReturn)
    {
        Flux<ApiCallRc> ret;
        CleanupData data = backupInfoMgr.l2lShippingFinished(stltRemote);
        if (data != null)
        {
            ret = startQueues(data.data, data.getTask());
        }
        else if (allowNullReturn)
        {
            ret = null;
        }
        else
        {
            ret = Flux.empty();
        }
        return ret;
    }

    public Flux<ApiCallRc> startQueues(
        BackupShippingSrcData data,
        StltRemoteCleanupTask task
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Backup shipping L2L: start queued snaps after shipping done",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> startQueuesInTransaction(data, task)
        );
    }

    private Flux<ApiCallRc> startQueuesInTransaction(
        BackupShippingSrcData data,
        StltRemoteCleanupTask task
    ) throws AccessDeniedException, DatabaseException
    {
        SnapshotDefinition snapDfn = null;
        if (data.getSrcSnapshot() != null && !data.getSrcSnapshot().isDeleted())
        {
            snapDfn = data.getSrcSnapshot().getSnapshotDefinition();
            snapDfn.getFlags().disableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
        }
        taskScheduleService.rescheduleAt(task, Task.END_TASK);
        ctrlTransactionHelper.commit();
        return queueHandler.handleBackupQueues(
            snapDfn,
            data.getLinstorRemote(),
            data.getStltRemote()
        );
    }

    private Flux<ApiCallRc> unsetTakeSnapshotInTransaction(BackupShippingSrcData dataRef)
    {
        AccessContext accCtx = peerAccCtx.get();
        Snapshot snap = dataRef.getSrcSnapshot();
        try
        {
            snap.setTakeSnapshot(accCtx, false);
            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "Cleaning up flags of snapshot after shippment started",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }

        return ctrlSatelliteUpdateCaller.updateSatellites(snap.getSnapshotDefinition(), notConnectedError())
            .transform(
                updateResponses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    updateResponses,
                    snap.getResourceName(),
                    "Cleanup of {1} on {0} "
                )
            );
    }
}
