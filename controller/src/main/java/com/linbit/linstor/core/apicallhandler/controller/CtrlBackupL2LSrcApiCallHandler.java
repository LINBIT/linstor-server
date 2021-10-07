package com.linbit.linstor.core.apicallhandler.controller;

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
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingResponse;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.LinstorRemote;
import com.linbit.linstor.core.objects.Remote;
import com.linbit.linstor.core.objects.Remote.RemoteType;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StltRemote;
import com.linbit.linstor.core.objects.StltRemoteControllerFactory;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.storage.utils.RestClient.RestOp;
import com.linbit.linstor.storage.utils.RestHttpClient;
import com.linbit.linstor.storage.utils.RestResponse;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.Pair;

import static com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller.notConnectedError;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlBackupL2LSrcApiCallHandler
{
    private final AccessContext sysCtx;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ErrorReporter errorReporter;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final StltRemoteControllerFactory stltRemoteFactory;

    private final BackupShippingRestClient restClient;
    private final CtrlBackupApiCallHandler ctrlBackupApiCallHandler;
    private final SystemConfRepository systemConfRepository;
    private final BackupInfoManager backupInfoMgr;
    private final RemoteRepository remoteRepo;
    private final CtrlSecurityObjects ctrlSecObjs;

    @Inject
    public CtrlBackupL2LSrcApiCallHandler(
        @SystemContext AccessContext sysCtxRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ErrorReporter errorReporterRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        StltRemoteControllerFactory stltRemoteFactoryRef,
        CtrlBackupApiCallHandler ctrlBackupApiCallHandlerRef,
        SystemConfRepository systemConfRepositoryRef,
        RemoteRepository remoteRepoRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlSecurityObjects ctrlSecObjsRef
    )
    {
        sysCtx = sysCtxRef;
        peerAccCtx = peerAccCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        errorReporter = errorReporterRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        stltRemoteFactory = stltRemoteFactoryRef;
        ctrlBackupApiCallHandler = ctrlBackupApiCallHandlerRef;
        systemConfRepository = systemConfRepositoryRef;
        remoteRepo = remoteRepoRef;
        backupInfoMgr = backupInfoMgrRef;
        ctrlSecObjs = ctrlSecObjsRef;

        restClient = new BackupShippingRestClient(errorReporterRef);
    }

    public Flux<ApiCallRc> shipBackup(
        String srcNodeNameRef,
        String srcRscNameRef,
        String linstorRemoteNameRef,
        String dstRscNameRef,
        @Nullable String dstNodeNameRef,
        @Nullable String dstNetIfNameRef,
        @Nullable String dstStorPoolRef,
        @Nullable Map<String, String> storPoolRenameRef,
        boolean downloadOnly
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Backup shipping L2L: Creating temporary SatelliteRemote",
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
                downloadOnly
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
        boolean downloadOnly
    )
    {

        Remote remote = ctrlApiDataLoader.loadRemote(linstorRemoteNameRef, true);
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

        long now = System.currentTimeMillis();
        String backupName = CtrlBackupApiCallHandler.generateNewSnapshotName(new Date(now));
        BackupShippingData data = new BackupShippingData(
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
            storPoolRenameRef,
            downloadOnly
        );

        /*
         * We first need to create a snapshot in our (source) cluster as when contacting the destination cluster, we
         * already have to send the backupPojo data (which includes properties of snapshot, -definition, etc...).
         * However, as the snapshot will require a StltRemote, we first have to create that and tell the satellite about
         * it.
         *
         * Next, we will contact the destination cluster and hope that we can start the shipment soon.
         * As this is an optimistic approach, we will have to delete our just created snapshot, -definition, etc if the
         * destination cluster refuses the shipment
         *
         * If the destination cluster agrees, the answer also contains IP and ports of the already waiting receiver. All
         * that is left to do on the source side is to set those IP / port into the previously create StltRemote object,
         * update the satellite, set the corresponding "you may start sending" flag and update the satellite again.
         */

        return scopeRunner.fluxInTransactionalScope(
            "Backup shipping L2L: Creating temporary SatelliteRemote",
            lockGuardFactory.create()
                .write(LockObj.REMOTE_MAP)
                .buildDeferred(),
            () -> createStltRemoteInTransaction(data)
        );
    }

    /**
     * Creates a StltRemote object, updates the satellites.
     *
     * Next Flux: create snapshot without starting shipment
     *
     * @param data
     *
     * @return
     */
    private Flux<ApiCallRc> createStltRemoteInTransaction(BackupShippingData data)
    {
        StltRemote stltRemote = createStltRemote(
            stltRemoteFactory,
            remoteRepo,
            peerAccCtx.get(),
            data.srcRscName,
            data.srcBackupName,
            new TreeMap<>()
        );

        data.stltRemote = stltRemote;

        ctrlTransactionHelper.commit();
        return ctrlSatelliteUpdateCaller.updateSatellites(stltRemote)
            .concatWith(
                scopeRunner.fluxInTransactionalScope(
                    "Backup shipping L2L: Creating source snapshot",
                    lockGuardFactory.create()
                        .read(LockObj.NODES_MAP)
                        .write(LockObj.RSC_DFN_MAP).buildDeferred(),
                    () -> shipBackupInTransaction(data)
                )
            );
    }

    /**
     * Create snapshot without starting shipment
     *
     * next Flux: contact remote controller
     *
     * @param data
     *
     * @return
     */
    private Flux<ApiCallRc> shipBackupInTransaction(BackupShippingData data)
    {
        Map<ExtTools, Version> requiredExtTools = new HashMap<>();
        requiredExtTools.put(ExtTools.SOCAT, null);
        Map<ExtTools, Version> optionalExtTools = new HashMap<>();
        optionalExtTools.put(ExtTools.ZSTD, null);
        Pair<Flux<ApiCallRc>, Snapshot> createSnapshot = ctrlBackupApiCallHandler.backupSnapshot(
            data.srcRscName,
            data.stltRemote.getName().displayValue,
            data.dstNodeName,
            data.srcBackupName,
            data.now,
            false,
            false,
            requiredExtTools,
            optionalExtTools,
            RemoteType.LINSTOR
        );
        data.srcSnapshot = createSnapshot.objB;
        data.srcNodeName = data.srcSnapshot.getNode().getName().displayValue;

        Flux<ApiCallRc> flux = createSnapshot.objA
            .concatWith(
                scopeRunner.fluxInTransactionalScope(
                    "Backup shipping L2L: Sending backup request to destination controller",
                    lockGuardFactory.create()
                        .read(LockObj.NODES_MAP)
                        .write(LockObj.RSC_DFN_MAP).buildDeferred(),
                    () -> prepareShippingInTransaction(data)
                )
            );

        return flux;
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
        Map<String, Integer> snapShipPortsRef
    )
    {
        StltRemote stltRemote;
        try
        {
            stltRemote = stltRemoteFactoryRef.create(
                accCtxRef,
                // add random uuid to avoid naming conflict
                RemoteName.createInternal(rscNameRef + "_" + snapshotNameRef + "_" + UUID.randomUUID().toString()),
                null,
                snapShipPortsRef
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
     * Contact remote controller and wait for destination satellite's IP/Port
     *
     * Next flux: update satellite remote with received IP/Port + start shipment
     *
     * @param data
     *
     * @return
     */
    private Flux<ApiCallRc> prepareShippingInTransaction(BackupShippingData data)
    {
        Flux<ApiCallRc> flux;
        try
        {
            BackupMetaDataPojo metaDataPojo = BackupShippingUtils.getBackupMetaDataPojo(
                peerAccCtx.get(),
                data.srcSnapshot,
                systemConfRepository.getStltConfForView(sysCtx),
                ctrlSecObjs.getEncKey(),
                ctrlSecObjs.getCryptHash(),
                ctrlSecObjs.getCryptSalt(),
                Collections.emptyMap(),
                null
            );

            NodeName srcSendingNodeName = data.srcSnapshot.getNodeName();
            backupInfoMgr.abortCreateAddL2LEntry(
                srcSendingNodeName,
                new SnapshotDefinition.Key(data.srcSnapshot.getSnapshotDefinition())
            );

            ExtToolsManager extToolsMgr = data.srcSnapshot.getNode().getPeer(sysCtx).getExtToolsManager();
            ExtToolsInfo zstd = extToolsMgr.getExtToolInfo(ExtTools.ZSTD);
            data.useZstd = zstd != null && zstd.isSupported();

            data.metaDataPojo = metaDataPojo;
            data.metaDataPojo.getRscDfn().getProps().put(
                InternalApiConsts.KEY_BACKUP_L2L_SRC_SNAP_DFN_UUID,
                data.srcSnapshot.getSnapshotDefinition().getUuid().toString()
            );

            // build list of possible base snapshots for incremental shipping
            for (SnapshotDefinition snapDfn : data.srcSnapshot.getResourceDefinition().getSnapshotDfns(sysCtx))
            {
                Snapshot snap = snapDfn.getSnapshot(sysCtx, srcSendingNodeName);
                if (snap != null)
                {
                    data.srcSnapDfnUuids.add(snapDfn.getUuid().toString());
                }
            }

            flux = Flux.merge(
                restClient.sendBackupRequest(data, peerAccCtx.get())
                    .map(
                        restResponse -> scopeRunner.fluxInTransactionalScope(
                            "Backup shipping L2L: Starting shipment",
                            lockGuardFactory.create()
                                .read(LockObj.NODES_MAP)
                                .write(LockObj.RSC_DFN_MAP).buildDeferred(),
                            () -> startShippingInTransaction(restResponse, data)
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

    /**
     * First flux: Updates stltRemote with received IP/Port<br/>
     *
     * Second flux: starts shipment
     *
     * @param responseRef
     * @param data
     *
     * @return
     */
    private Flux<ApiCallRc> startShippingInTransaction(
        BackupShippingResponse responseRef,
        BackupShippingData data
    )
    {
        Flux<ApiCallRc> flux;
        try
        {
            if (responseRef.canReceive)
            {
                AccessContext accCtx = peerAccCtx.get();

                StltRemote stltRemote = data.stltRemote;
                stltRemote.setIp(accCtx, responseRef.dstStltIp);
                stltRemote.setAllPorts(accCtx, responseRef.dstStltPorts);

                stltRemote.useZstd(accCtx, responseRef.useZstd && data.useZstd);

                ctrlTransactionHelper.commit();
                flux = ctrlSatelliteUpdateCaller.updateSatellites(stltRemote);

                Snapshot snap = data.srcSnapshot;

                snap.getFlags().enableFlags(accCtx, Snapshot.Flags.BACKUP_SOURCE);
                snap.setTakeSnapshot(accCtx, true);// needed by source-satellite to actually start sending

                if (responseRef.srcSnapDfnUuid != null && !responseRef.srcSnapDfnUuid.isEmpty())
                {
                    SnapshotDefinition prevSnapDfn = null;
                    UUID prevSnapUuid = UUID.fromString(responseRef.srcSnapDfnUuid);
                    for (SnapshotDefinition snapDfn : snap.getResourceDefinition().getSnapshotDfns(accCtx))
                    {
                        if (snapDfn.getUuid().equals(prevSnapUuid)) {
                            prevSnapDfn = snapDfn;
                            break;
                        }
                    }
                    if (prevSnapDfn == null) {
                        throw new ImplementationError(
                            "SnapshotDefinition selected by destination cluster not found locally: " +
                                responseRef.srcSnapDfnUuid
                        );
                    }

                    ctrlBackupApiCallHandler.setPropsIncremantaldependendProps(
                        snap.getSnapshotDefinition().getName().displayValue,
                        true,
                        prevSnapDfn,
                        snap
                    );
                }

                ctrlTransactionHelper.commit();
                flux = flux.concatWith(
                    ctrlSatelliteUpdateCaller.updateSatellites(snap.getSnapshotDefinition(), notConnectedError())
                        .transform(
                            updateResponses -> CtrlResponseUtils.combineResponses(
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
                        "Remote '" + data.linstorRemote.getName().displayValue + "': ",
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

    private Flux<ApiCallRc> unsetTakeSnapshotInTransaction(BackupShippingData dataRef)
    {
        AccessContext accCtx = peerAccCtx.get();
        Snapshot snap = dataRef.srcSnapshot;
        try
        {
            snap.setTakeSnapshot(accCtx, true);
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
                    updateResponses,
                    snap.getResourceName(),
                    "Cleanup of {1} on {0} "
                )
            );
    }

    private class BackupShippingRestClient
    {
        private final ErrorReporter errorReporter;
        private final RestHttpClient restClient;
        private final ObjectMapper objMapper;

        public BackupShippingRestClient(ErrorReporter errorReporterRef)
        {
            errorReporter = errorReporterRef;
            restClient = new RestHttpClient(errorReporterRef);
            objMapper = new ObjectMapper();
        }

        public Flux<BackupShippingResponse> sendBackupRequest(BackupShippingData data, AccessContext accCtx)
        {
            Flux<BackupShippingResponse> flux = Flux.create(fluxSink ->
            {
                Runnable run = () ->
                {
                    try
                    {
                        final int responseOk = Response.Status.OK.getStatusCode();
                        final int notFound = Response.Status.NOT_FOUND.getStatusCode();
                        final int badRequest = Response.Status.BAD_REQUEST.getStatusCode();
                        final int internalServerError = Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();

                        String restURL = data.linstorRemote.getUrl(accCtx).toExternalForm() +
                            "/v1/internal/backups/requestShip";
                        RestResponse<BackupShippingResponse> response = restClient.execute(
                            null,
                            RestOp.POST,
                            restURL,
                            Collections.emptyMap(),
                            objMapper.writeValueAsString(
                                new BackupShippingRequest(
                                    LinStor.VERSION_INFO_PROVIDER.getSemanticVersion(),
                                    data.metaDataPojo,
                                    data.srcBackupName,
                                    data.srcClusterId,
                                    data.srcSnapDfnUuids,
                                    data.dstRscName,
                                    data.dstNodeName,
                                    data.dstNetIfName,
                                    data.dstStorPool,
                                    data.storPoolRename,
                                    data.useZstd,
                                    data.downloadOnly
                                )
                            ),
                            Arrays.asList(responseOk, notFound, badRequest, internalServerError),
                            BackupShippingResponse.class
                        );

                        if (response.getStatusCode() != responseOk)
                        {
                            ApiCallRcImpl apiCallRc;

                            if (response.getStatusCode() == internalServerError)
                            {
                                apiCallRc = ApiCallRcImpl.copyAndPrefix(
                                    "Remote '" + data.linstorRemote.getName().displayValue + "': ",
                                    response.getData().responses
                                );
                            }
                            else
                            {
                                apiCallRc = new ApiCallRcImpl();
                                apiCallRc.addEntry(
                                    ApiCallRcImpl.entryBuilder(
                                        ApiConsts.FAIL_BACKUP_INCOMPATIBLE_VERSION,
                                        "Destination controller incompatible"
                                    )
                                        .setCause("Probably the destination controller is not recent enough")
                                        .setCorrection(
                                            "Make sure the destination cluster is on the same version as the current cluster (" +
                                                LinStor.VERSION_INFO_PROVIDER.getVersion() + ")"
                                        )
                                        .build()
                                );
                            }

                            fluxSink.error(new ApiRcException(apiCallRc));
                        }
                        else
                        {
                            fluxSink.next(response.getData());
                            fluxSink.complete();
                        }
                    }
                    catch (StorageException | IOException | AccessDeniedException exc)
                    {
                        errorReporter.reportError(exc);
                        fluxSink.error(exc);
                    }
                };
                new Thread(run).start();
            }
            );
            return flux;
        }
    }

    private class BackupShippingData
    {
        private final HashSet<String> srcSnapDfnUuids = new HashSet<>();
        private final String srcClusterId;
        private String srcNodeName;
        private final String srcRscName;
        private final String srcBackupName;
        private final long now;
        private Snapshot srcSnapshot;
        private final LinstorRemote linstorRemote;
        private boolean useZstd;
        private boolean downloadOnly;

        private BackupMetaDataPojo metaDataPojo;
        private final String dstRscName;
        private String dstNodeName;
        private String dstNetIfName;
        private String dstStorPool;
        private final Map<String, String> storPoolRename;

        private StltRemote stltRemote;

        public BackupShippingData(
            String srcClusterIdRef,
            String srcNodeNameRef,
            String srcRscNameRef,
            String srcBackupNameRef,
            long nowRef,
            LinstorRemote linstorRemoteRef,
            String dstRscNameRef,
            String dstNodeNameRef,
            String dstNetIfNameRef,
            String dstStorPoolRef,
            Map<String, String> storPoolRenameRef,
            boolean downloadOnlyRef
        )
        {
            srcClusterId = srcClusterIdRef;
            srcNodeName = srcNodeNameRef;
            srcRscName = srcRscNameRef;
            srcBackupName = srcBackupNameRef;
            now = nowRef;
            linstorRemote = linstorRemoteRef;
            dstRscName = dstRscNameRef;
            dstNodeName = dstNodeNameRef;
            dstNetIfName = dstNetIfNameRef;
            dstStorPool = dstStorPoolRef;
            storPoolRename = storPoolRenameRef;
            downloadOnly = downloadOnlyRef;
        }
    }
}
