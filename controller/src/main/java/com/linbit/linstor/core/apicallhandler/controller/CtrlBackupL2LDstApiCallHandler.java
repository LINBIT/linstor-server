package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.SecretGenerator;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingReceiveRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingResponse;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.LinstorRemote;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Remote;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StltRemote;
import com.linbit.linstor.core.objects.StltRemoteControllerFactory;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlBackupL2LDstApiCallHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ErrorReporter errorReporter;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlBackupApiCallHandler backupApiCallHandler;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final DynamicNumberPool snapshotShippingPortPool;
    private final StltRemoteControllerFactory stltRemoteControllerFactory;
    private final RemoteRepository remoteRepo;
    private final SystemConfRepository systemConfRepository;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlBackupL2LSrcApiCallHandler.BackupShippingRestClient backupShippingRestClient;

    @Inject
    public CtrlBackupL2LDstApiCallHandler(
        @SystemContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ErrorReporter errorReporterRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlBackupApiCallHandler backupApiCallHandlerRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        @Named(NumberPoolModule.SNAPSHOPT_SHIPPING_PORT_POOL) DynamicNumberPool snapshotShippingPortPoolRef,
        StltRemoteControllerFactory stltRemoteControllerFactoryRef,
        RemoteRepository remoteRepoRef,
        SystemConfRepository systemConfRepositoryRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlBackupL2LSrcApiCallHandler helperReference
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        errorReporter = errorReporterRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        backupApiCallHandler = backupApiCallHandlerRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        snapshotShippingPortPool = snapshotShippingPortPoolRef;
        stltRemoteControllerFactory = stltRemoteControllerFactoryRef;
        remoteRepo = remoteRepoRef;
        systemConfRepository = systemConfRepositoryRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        backupInfoMgr = backupInfoMgrRef;
        backupShippingRestClient = helperReference.new BackupShippingRestClient(errorReporter);
    }

    public Flux<BackupShippingResponse> startReceiving(
        int[] srcVersionRef,
        String dstRscNameRef,
        BackupMetaDataPojo metaDataRef,
        String srcBackupNameRef,
        String srcClusterIdRef,
        Set<String> srcSnapDfnUuidsRef,
        @Nullable String dstNodeNameRef,
        @Nullable String dstNetIfNameRef,
        @Nullable String dstStorPoolRef,
        @Nullable Map<String, String> storPoolRenameMapRef,
        boolean useZstd,
        boolean downloadOnly,
        String srcL2LRemoteName
    )
    {
        Flux<BackupShippingResponse> flux;
        LinstorRemote srcRemote = loadLinstorRemote(srcClusterIdRef);
        if (!LinStor.VERSION_INFO_PROVIDER.equalsVersion(srcVersionRef[0], srcVersionRef[1], srcVersionRef[2]))
        {
            flux = Flux.just(
                new BackupShippingResponse(
                    false,
                    ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_BACKUP_INCOMPATIBLE_VERSION,
                        "Incompatible versions. Source version: " + srcVersionRef[0] + "." + srcVersionRef[1] + "." +
                        srcVersionRef[2] + ". Destination (local) version: " +
                        LinStor.VERSION_INFO_PROVIDER.getVersion()
                    )
                )
            );
        }
        else if (srcRemote == null)
        {
            flux = Flux.just(
                new BackupShippingResponse(
                    false,
                    ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_BACKUP_UNKNOWN_CLUSTER,
                        "Unknown Cluster. Source Cluster ID: " + srcClusterIdRef
                    )
                )
            );
        }
        else
        {
            Map<String, Integer> snapShipPorts = new TreeMap<>(); // Key: vlmNr + rscLayerSuffx, value: portNr
            try
            {
                RscLayerDataApi layerData = metaDataRef.getLayerData();
                allocatePortNumbers(layerData, snapShipPorts);

                BackupShippingData data = new BackupShippingData(
                    srcVersionRef,
                    srcL2LRemoteName,
                    srcRemote.getUrl(apiCtx).toExternalForm(),
                    dstRscNameRef,
                    metaDataRef,
                    srcBackupNameRef,
                    srcClusterIdRef,
                    srcSnapDfnUuidsRef,
                    dstNodeNameRef,
                    dstNetIfNameRef,
                    dstStorPoolRef,
                    storPoolRenameMapRef,
                    snapShipPorts,
                    useZstd,
                    downloadOnly
                );

                flux = scopeRunner.fluxInTransactionalScope(
                    "Backupshipping L2L start receive",
                    lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP).buildDeferred(),
                    () -> startReceivingInTransaction(data)
                ).map(startInfo -> snapshotToResponse(data, startInfo));
                // .onErrorResume(err -> errorToResponse(err));
            }
            catch (ExhaustedPoolException exc)
            {
                flux = Flux.just(
                    new BackupShippingResponse(
                        false,
                        ApiCallRcImpl.singleApiCallRc(
                            ApiConsts.FAIL_POOL_EXHAUSTED_BACKUP_SHIPPING_TCP_PORT,
                            "Shipping port range exhausted"
                        )
                    )
                );
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }

        return flux;
    }

    private LinstorRemote loadLinstorRemote(String srcClusterIdRef)
    {
        try
        {
            LinstorRemote ret = null;
            for (Remote remote : remoteRepo.getMapForView(apiCtx).values())
            {
                if (remote instanceof LinstorRemote)
                {
                    LinstorRemote linRem = (LinstorRemote) remote;
                    if(linRem.getClusterId(apiCtx).equals(UUID.fromString(srcClusterIdRef))){
                        ret = linRem;
                        break;
                    }
                }
            }
            return ret;
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void allocatePortNumbers(RscLayerDataApi layerDataRef, Map<String, Integer> ports)
        throws ExhaustedPoolException
    {
        for (RscLayerDataApi child : layerDataRef.getChildren())
        {
            allocatePortNumbers(child, ports);
        }
        if (layerDataRef.getLayerKind().equals(DeviceLayerKind.STORAGE))
        {
            if (RscLayerSuffixes.shouldSuffixBeShipped(layerDataRef.getRscNameSuffix()))
            {
                for (VlmLayerDataApi vlm : layerDataRef.getVolumeList())
                {
                    ports
                        .put(vlm.getVlmNr() + layerDataRef.getRscNameSuffix(), snapshotShippingPortPool.autoAllocate());
                }
            }
        }
    }

    public Flux<ApiCallRc> reallocatePorts(String remoteName, String snapName, String rscName, Set<Integer> ports)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Backup shipping L2L: reallocate ports",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP).buildDeferred(),
            () -> reallocatePortsInTransaction(remoteName, snapName, rscName, ports)
        );
    }

    private Flux<ApiCallRc> reallocatePortsInTransaction(
        String remoteName,
        String snapName,
        String rscName,
        Set<Integer> ports
    )
    {
        try
        {
            Remote remote = remoteRepo.get(apiCtx, new RemoteName(remoteName, true));
            if (remote instanceof StltRemote)
            {
                StltRemote stltRemote = (StltRemote) remote;
                Map<String, Integer> newPorts = new TreeMap<>();
                for (Entry<String, Integer> portEntry : stltRemote.getPorts(apiCtx).entrySet())
                {
                    if (ports.contains(portEntry.getValue()))
                    {
                        newPorts.put(portEntry.getKey(), snapshotShippingPortPool.autoAllocate());
                    }
                    else
                    {
                        newPorts.put(portEntry.getKey(), portEntry.getValue());
                    }
                }
                stltRemote.setAllPorts(apiCtx, newPorts);
                ctrlTransactionHelper.commit();
                SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapName, true);
                return ctrlSatelliteUpdateCaller.updateSatellites(stltRemote)
                    .concatWith(
                        ctrlSatelliteUpdateCaller.updateSatellites(
                            snapDfn,
                            CtrlSatelliteUpdateCaller.notConnectedWarn()
                        ).transform(
                            responses -> CtrlResponseUtils.combineResponses(
                                responses,
                                LinstorParsingUtils.asRscName(rscName),
                                "Restarting receiving of backup ''" + snapName + "'' of {1} on {0}"
                            )
                        )
                    );
            }
            else
            {
                throw new ImplementationError(
                    "Expected type StltRemote, instead got " + remote.getClass().getCanonicalName()
                );
            }
        }
        catch (AccessDeniedException | InvalidNameException | ExhaustedPoolException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Flux<BackupShippingStartInfo> startReceivingInTransaction(BackupShippingData data)
    {
        String clusterHash = getSrcClusterShortId(data.srcClusterId);

        SnapshotName snapName = LinstorParsingUtils.asSnapshotName(data.srcBackupName + "_" + clusterHash);
        StltRemote stltRemote = CtrlBackupL2LSrcApiCallHandler.createStltRemote(
            stltRemoteControllerFactory,
            remoteRepo,
            apiCtx,
            data.dstRscName,
            snapName.displayValue,
            data.snapShipPorts
        );

        data.snapName = snapName;
        data.stltRemote = stltRemote;

        Map<String, String> snapPropsFromSource = data.metaData.getRsc().getProps();
        snapPropsFromSource.put(InternalApiConsts.KEY_BACKUP_L2L_SRC_CLUSTER_UUID, data.srcClusterId);
        snapPropsFromSource.put(InternalApiConsts.KEY_BACKUP_L2L_SRC_CLUSTER_SHORT_HASH, clusterHash);
        snapPropsFromSource.put(
            ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" + InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
            stltRemote.getName().displayValue
        );

        ctrlTransactionHelper.commit();

        return ctrlSatelliteUpdateCaller.updateSatellites(stltRemote)
            .thenMany(
                freeCapacityFetcher.fetchThinFreeCapacities(
                    data.dstNodeName == null ?
                        Collections.emptySet() :
                        Collections.singleton(LinstorParsingUtils.asNodeName(data.dstNodeName))
                ).flatMapMany(
                    thinFreeCapacities -> scopeRunner.fluxInTransactionalScope(
                        "restore backup",
                        lockGuardFactory.buildDeferred(
                            LockType.WRITE,
                            LockObj.NODES_MAP,
                            LockObj.RSC_DFN_MAP
                        ),
                        () -> backupApiCallHandler.restoreBackupL2LInTransaction(
                            thinFreeCapacities,
                            data
                        )
                    )
                )
        );
    }

    private String getSrcClusterShortId(String srcClusterIdRef)
    {
        String ret;
        try
        {
            Props props = systemConfRepository.getCtrlConfForChange(apiCtx);
            ret = props.getProp(srcClusterIdRef, ApiConsts.NAMESPC_CLUSTER_REMOTE);
            if (ret == null)
            {
                ret = generateClusterShortId();

                Optional<Props> namespace = props.getNamespace(ApiConsts.NAMESPC_CLUSTER_REMOTE);
                if (namespace.isPresent())
                {
                    HashSet<String> existingHashes = new HashSet<>(namespace.get().values());

                    while (existingHashes.contains(ret))
                    {
                        ret = generateClusterShortId();
                    }
                }

                props.setProp(srcClusterIdRef, ret, ApiConsts.NAMESPC_CLUSTER_REMOTE);
            }
        }
        catch (AccessDeniedException | InvalidKeyException | DatabaseException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    public static String generateClusterShortId()
    {
        return new BigInteger(SecretGenerator.generateSecret(5)).abs().toString(36);
    }

    private BackupShippingResponse snapshotToResponse(
        BackupShippingData dataRef,
        BackupShippingStartInfo startInfo
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        Snapshot snap = startInfo.apiCallRcWithSnapshot.extractApiCallRc(responses);
        if (snap.isDeleted())
        {
            responses.addEntry(
                "Error while trying to start receiving, Snapshot has already been deleted due to it.",
                ApiConsts.FAIL_UNKNOWN_ERROR
            );
            return new BackupShippingResponse(
                false,
                responses
            );
        }

        return new BackupShippingResponse(
            true,
            responses
        );
    }

    public Flux<ApiCallRc> sendBackupShippingReceiveRequest(
        String rscName,
        String snapName,
        String nodeName,
        String remoteName
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Backup shipping L2L: send backup receive request to source ctrl",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP).buildDeferred(),
            () -> sendBackupShippingReceiveRequestInTransaction(rscName, snapName, nodeName, remoteName)
        );
    }

    private Flux<ApiCallRc> sendBackupShippingReceiveRequestInTransaction(
        String rscName,
        String snapName,
        String nodeName,
        String remoteName
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        try
        {
            NetInterface netIf = null;
            Remote remote = remoteRepo.get(apiCtx, new RemoteName(remoteName, true));
            Map<String, Integer> ports;
            if (remote instanceof StltRemote)
            {
                ports = ((StltRemote) remote).getPorts(apiCtx);
            }
            else
            {
                throw new ImplementationError(
                    "Expected type StltRemote, instead got " + remote.getClass().getCanonicalName()
                );
            }
            Snapshot snap = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapName, true)
                .getSnapshot(apiCtx, new NodeName(nodeName));
            BackupShippingData data = backupInfoMgr.getL2LDstData(snap);
            backupInfoMgr.removeL2LDstData(snap);
            Node node = snap.getNode();
            if (data.dstNetIfName != null)
            {
                netIf = node.getNetInterface(
                    apiCtx,
                    LinstorParsingUtils.asNetInterfaceName(data.dstNetIfName)
                );
            }
            if (netIf == null)
            {
                netIf = node.iterateNetInterfaces(apiCtx).next();
            }


            boolean useZstd = data.stltRemote.useZstd(apiCtx);

            return backupShippingRestClient.sendBackupReceiveRequest(
                new BackupShippingReceiveRequest(
                    true,
                    responses,
                    data.srcL2LRemoteName,
                    data.srcL2LRemoteUrl,
                    netIf.getAddress(apiCtx).getAddress(),
                    ports,
                    data.incrBaseSnapDfnUuid,
                    useZstd
                ),
                apiCtx
            ).thenMany(Flux.empty()); // request is from stlt, so instead of converting from JsonGenTypes.ApiCallRc to
                                      // ApiCallRc the response gets ignored
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (InvalidNameException exc)
        {
            errorReporter.reportError(exc);
            responses = ApiCallRcImpl.singleApiCallRc(
                ApiConsts.FAIL_INVLD_NODE_NAME,
                "Invalid node name given",
                exc.getMessage()
            );
        }
        return Flux.just(responses);
    }

    private Flux<BackupShippingResponse> errorToResponse(Throwable error)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        return Flux.just(
            new BackupShippingResponse(
                false,
                responses
            )
        ).thenMany(Flux.error(error));
    }

    static class BackupShippingStartInfo
    {
        private final ApiCallRcWith<Snapshot> apiCallRcWithSnapshot;
        @Nullable
        private final Snapshot incrBaseSnapshot;

        public BackupShippingStartInfo(ApiCallRcWith<Snapshot> apiCallRcWithSnapshotRef, Snapshot incrBaseSnapshotRef)
        {
            super();
            apiCallRcWithSnapshot = apiCallRcWithSnapshotRef;
            incrBaseSnapshot = incrBaseSnapshotRef;
        }
    }

    public class BackupShippingData
    {
        public StltRemote stltRemote;
        public SnapshotName snapName;
        public final String srcL2LRemoteName;
        public final String srcL2LRemoteUrl;
        public final int[] srcVersion;
        public final String dstRscName;
        public final BackupMetaDataPojo metaData;
        public final String srcBackupName;
        public final String srcClusterId;
        public final Set<String> srcSnapDfnUuids;
        public String incrBaseSnapDfnUuid;
        public String dstNodeName;
        public String dstNetIfName;
        public String dstStorPool;
        public Map<String, String> storPoolRenameMap;
        public final Map<String, Integer> snapShipPorts;
        public boolean useZstd;
        public boolean downloadOnly;

        public BackupShippingData(
            int[] srcVersionRef,
            String srcL2LRemoteNameRef,
            String srcL2LRemoteUrlRef,
            String dstRscNameRef,
            BackupMetaDataPojo metaDataRef,
            String srcBackupNameRef,
            String srcClusterIdRef,
            Set<String> srcSnapDfnUuidsRef,
            String dstNodeNameRef,
            String dstNetIfNameRef,
            String dstStorPoolRef,
            Map<String, String> storPoolRenameMapRef,
            Map<String, Integer> snapShipPortsRef,
            boolean useZstdRef,
            boolean downloadOnlyRef
        )
        {
            srcVersion = srcVersionRef;
            srcL2LRemoteName = srcL2LRemoteNameRef;
            srcL2LRemoteUrl = srcL2LRemoteUrlRef;
            dstRscName = dstRscNameRef;
            metaData = metaDataRef;
            srcBackupName = srcBackupNameRef;
            srcClusterId = srcClusterIdRef;
            srcSnapDfnUuids = srcSnapDfnUuidsRef;
            dstNodeName = dstNodeNameRef;
            dstNetIfName = dstNetIfNameRef;
            dstStorPool = dstStorPoolRef;
            storPoolRenameMap = storPoolRenameMapRef;
            snapShipPorts = snapShipPortsRef;
            useZstd = useZstdRef;
            downloadOnly = downloadOnlyRef;
        }
    }
}
