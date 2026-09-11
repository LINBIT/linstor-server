package com.linbit.linstor.core.apicallhandler.controller.backup;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.crypto.SecretGenerator;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingPrepareAbortRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingRestClient;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingDstData;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingReceiveRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingResponse;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingResponsePrevSnap;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.core.objects.remotes.StltRemoteControllerFactory;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlBackupL2LDstApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ErrorReporter errorReporter;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlBackupRestoreApiCallHandler backupRestoreApiCallHandler;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final DynamicNumberPool snapshotShippingPortPool;
    private final ModularCryptoProvider cryptoProvider;
    private final StltRemoteControllerFactory stltRemoteControllerFactory;
    private final RemoteRepository remoteRepo;
    private final SystemConfRepository systemConfRepository;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final BackupInfoManager backupInfoMgr;
    private final BackupShippingRestClient backupShippingRestClient;
    private final Provider<Peer> peerProvider;
    private final CtrlBackupApiHelper backupHelper;

    /**
     * LookUpTable so that we can tell the src-controller its own stltRemoteName which it needs
     * to find the corresponding BackupShippingData
     */
    private final Map<RemoteName, String> dstToSrcStltRemoteNameMap;

    @Inject
    public CtrlBackupL2LDstApiCallHandler(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ErrorReporter errorReporterRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlBackupRestoreApiCallHandler backupRestoreApiCallHandlerRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        @Named(NumberPoolModule.BACKUP_SHIPPING_PORT_POOL) DynamicNumberPool snapshotShippingPortPoolRef,
        ModularCryptoProvider cryptoProviderRef,
        StltRemoteControllerFactory stltRemoteControllerFactoryRef,
        RemoteRepository remoteRepoRef,
        SystemConfRepository systemConfRepositoryRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        BackupInfoManager backupInfoMgrRef,
        BackupShippingRestClient restClientRef,
        Provider<Peer> peerProviderRef,
        CtrlBackupApiHelper backupHelperRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        errorReporter = errorReporterRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        backupRestoreApiCallHandler = backupRestoreApiCallHandlerRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        snapshotShippingPortPool = snapshotShippingPortPoolRef;
        cryptoProvider = cryptoProviderRef;
        stltRemoteControllerFactory = stltRemoteControllerFactoryRef;
        remoteRepo = remoteRepoRef;
        systemConfRepository = systemConfRepositoryRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        backupInfoMgr = backupInfoMgrRef;
        peerProvider = peerProviderRef;
        backupHelper = backupHelperRef;
        backupShippingRestClient = restClientRef;

        dstToSrcStltRemoteNameMap = new HashMap<>();
    }

    public Flux<BackupShippingResponsePrevSnap> getPrevSnap(
        int[] srcVersionRef,
        String srcClusterIdRef,
        String dstRscName,
        Set<String> srcSnapDfnUuids,
        @Nullable String dstNodeName
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Backup shippin L2L: get base snap",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP, LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> getPrevSnapInTransaction(srcVersionRef, srcClusterIdRef, dstRscName, srcSnapDfnUuids, dstNodeName)
        );
    }

    private Flux<BackupShippingResponsePrevSnap> getPrevSnapInTransaction(
        int[] srcVersionRef,
        String srcClusterIdRef,
        String dstRscName,
        Set<String> srcSnapDfnUuids,
        @Nullable String dstNodeName
    )
    {
        Flux<BackupShippingResponsePrevSnap> flux = null;
        ApiCallRcImpl rc = isClusterAllowedContact(srcVersionRef, srcClusterIdRef);
        if (rc != null)
        {
            flux = Flux.just(new BackupShippingResponsePrevSnap(false, null, false, null, null, rc));
        }
        else
        {
            rc = new ApiCallRcImpl();
            try
            {
                // good case, continue
                // search for incremental base
                Snapshot incrementalBaseSnap = null;
                @Nullable ResourceDefinition targetRscDfn = ctrlApiDataLoader.loadRscDfnOrNull(dstRscName);
                if (targetRscDfn != null)
                {
                    incrementalBaseSnap = backupRestoreApiCallHandler.getIncrementalBaseL2LPrivileged(
                        targetRscDfn,
                        srcSnapDfnUuids,
                        dstNodeName,
                        rc
                    );
                }
                boolean resetData = targetRscDfn == null || targetRscDfn.getResourceCount() == 0;
                // if baseSnap not null => incremental sync
                if (incrementalBaseSnap != null)
                {
                    // this prop already has the namespace Backup/Target included in the key, no worries there
                    String srcSnapDfnUuid = incrementalBaseSnap.getSnapshotDefinition()
                        .getSnapDfnProps()
                        .getProp(
                            InternalApiConsts.KEY_BACKUP_L2L_SRC_SNAP_DFN_UUID
                        );
                    flux = Flux.just(
                        new BackupShippingResponsePrevSnap(
                            true,
                            srcSnapDfnUuid,
                            resetData,
                            incrementalBaseSnap.getSnapshotName().displayValue,
                            incrementalBaseSnap.getNodeName().displayValue,
                            rc
                        )
                    );
                }
                else
                {
                    flux = Flux.just(
                        new BackupShippingResponsePrevSnap(
                            true,
                            null,
                            resetData,
                            null,
                            null,
                            rc
                        )
                    );
                }
            }
            catch (InvalidKeyException exc)
            {
                throw new ImplementationError(exc);
            }
        }
        return flux;
    }

    private @Nullable ApiCallRcImpl isClusterAllowedContact(int[] srcVersionRef, String srcClusterIdRef)
    {
        LinstorRemote srcRemote = loadLinstorRemote(srcClusterIdRef);
        return isClusterAllowedContact(srcVersionRef, srcClusterIdRef, srcRemote);
    }

    private @Nullable ApiCallRcImpl isClusterAllowedContact(
        int[] srcVersionRef,
        String srcClusterIdRef,
        LinstorRemote srcRemote
    )
    {
        ApiCallRcImpl rc = null;
        if (!LinStor.VERSION_INFO_PROVIDER.equalsVersion(srcVersionRef[0], srcVersionRef[1], srcVersionRef[2]))
        {
            rc = ApiCallRcImpl.singleApiCallRc(
                ApiConsts.FAIL_BACKUP_INCOMPATIBLE_VERSION,
                "Incompatible versions. Source version: " + srcVersionRef[0] + "." + srcVersionRef[1] + "." +
                    srcVersionRef[2] + ". Destination (local) version: " +
                    LinStor.VERSION_INFO_PROVIDER.getVersion()
            );
        }
        else if (srcRemote == null)
        {
            rc = ApiCallRcImpl.singleApiCallRc(
                ApiConsts.FAIL_BACKUP_UNKNOWN_CLUSTER,
                "Unknown Cluster. Source Cluster ID: " + srcClusterIdRef
            );
        }
        return rc;
    }

    public Flux<BackupShippingResponse> startReceiving(
        int[] srcVersionRef,
        String dstRscNameRef,
        BackupMetaDataPojo metaDataRef,
        String srcBackupNameRef,
        String srcClusterIdRef,
        @Nullable String dstNodeNameRef,
        @Nullable String dstNetIfNameRef,
        @Nullable String dstStorPoolRef,
        @Nullable Map<String, String> storPoolRenameMapRef,
        @Nullable String dstRscGrpRef,
        boolean useZstd,
        boolean downloadOnly,
        boolean forceRestore,
        String srcL2LRemoteName, // linstorRemoteName, not StltRemoteName
        String srcStltRemoteName,
        String srcRscName,
        boolean resetData,
        @Nullable String dstBaseSnapName,
        @Nullable String dstActualNodeName,
        boolean forceRscGrpRef
    )
    {
        Flux<BackupShippingResponse> flux;
        LinstorRemote srcRemote = loadLinstorRemote(srcClusterIdRef);

        ApiCallRcImpl rc = isClusterAllowedContact(srcVersionRef, srcClusterIdRef, srcRemote);
        if (rc != null)
        {
            flux = Flux.just(new BackupShippingResponse(false, rc));
        }
        else
        {
            Map<String, Integer> snapShipPorts = new TreeMap<>(); // Key: vlmNr + rscLayerSuffx, value: portNr
            try
            {
                RscLayerDataApi layerData = metaDataRef.getLayerData();
                allocatePortNumbers(layerData, snapShipPorts);
                BackupShippingDstData data = new BackupShippingDstData(
                    srcVersionRef,
                    srcL2LRemoteName,
                    srcStltRemoteName,
                    srcRemote.getUrl().toExternalForm(),
                    dstRscNameRef,
                    metaDataRef,
                    srcBackupNameRef,
                    srcClusterIdRef,
                    srcRscName,
                    dstNodeNameRef,
                    dstNetIfNameRef,
                    dstStorPoolRef,
                    storPoolRenameMapRef,
                    dstRscGrpRef,
                    snapShipPorts,
                    useZstd,
                    downloadOnly,
                    forceRestore,
                    resetData,
                    dstBaseSnapName,
                    dstActualNodeName,
                    forceRscGrpRef
                );
                flux = scopeRunner.fluxInTransactionalScope(
                    "Backupshipping L2L start receive",
                    lockGuardFactory.create()
                        .read(LockObj.NODES_MAP)
                        .write(LockObj.RSC_DFN_MAP, LockObj.REMOTE_MAP)
                        .buildDeferred(),
                    () -> startReceivingInTransaction(data, srcRemote)
                ).map(this::snapshotToResponse);
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
        }
        return flux;
    }

    private @Nullable LinstorRemote loadLinstorRemote(String srcClusterIdRef)
    {
        LinstorRemote ret = null;
        UUID srcClusterId = UUID.fromString(srcClusterIdRef);
        for (AbsRemote remote : remoteRepo.getMapForView().values())
        {
            if (remote instanceof LinstorRemote linRem)
            {
                if (Objects.equals(linRem.getClusterId(), srcClusterId))
                {
                    ret = linRem;
                    break;
                }
            }
        }
        return ret;
    }

    private void allocatePortNumbers(RscLayerDataApi layerDataRef, Map<String, Integer> ports)
        throws ExhaustedPoolException
    {
        for (RscLayerDataApi child : layerDataRef.getChildren())
        {
            allocatePortNumbers(child, ports);
        }
        if (
            layerDataRef.getLayerKind().equals(DeviceLayerKind.STORAGE) &&
                RscLayerSuffixes.shouldSuffixBeShipped(layerDataRef.getRscNameSuffix())
        )
        {
            for (VlmLayerDataApi vlm : layerDataRef.getVolumeList())
            {
                ports.put(vlm.getVlmNr() + layerDataRef.getRscNameSuffix(), snapshotShippingPortPool.autoAllocate());
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
            AbsRemote remote = remoteRepo.get(new RemoteName(remoteName, true));
            if (remote instanceof StltRemote stltRemote)
            {
                Map<String, Integer> newPorts = new TreeMap<>();
                for (Entry<String, Integer> portEntry : stltRemote.getPorts().entrySet())
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
                stltRemote.setAllPorts(newPorts);
                ctrlTransactionHelper.commit();
                SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapName);

                return ctrlSatelliteUpdateCaller.updateSatellites(stltRemote)
                    .concatWith(
                        backupHelper.startStltCleanup(
                            peerProvider.get(),
                            rscName,
                            snapName,
                            stltRemote.getLinstorRemoteName().displayValue
                        )
                    )
                    .concatWith(
                        ctrlSatelliteUpdateCaller.updateSatellites(
                            snapDfn,
                            CtrlSatelliteUpdateCaller.notConnectedWarn()
                        ).transform(
                            responses -> CtrlResponseUtils.combineResponses(
                                errorReporter,
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
        catch (InvalidNameException | ExhaustedPoolException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Flux<BackupShippingStartInfo> startReceivingInTransaction(BackupShippingDstData data, AbsRemote srcRemote)
    {
        @Nullable String clusterHash = getSrcClusterShortId(data.getSrcClusterId());

        SnapshotName snapName = LinstorParsingUtils.asSnapshotName(
            clusterHash != null ? (data.getSrcBackupName() + "_" + clusterHash) : data.getSrcBackupName()
        );
        StltRemote stltRemote = CtrlBackupL2LSrcApiCallHandler.createStltRemote(
            stltRemoteControllerFactory,
            remoteRepo,
            data.getDstRscName(),
            snapName.displayValue,
            data.getSnapShipPorts(),
            srcRemote.getName(),
            null, // we are on dst, the node is only needed on src
            data.getSrcRscName()
        );

        data.setSnapName(snapName);
        data.setStltRemote(stltRemote);

        synchronized (dstToSrcStltRemoteNameMap)
        {
            dstToSrcStltRemoteNameMap.put(stltRemote.getName(), data.getSrcStltRemoteName());
        }
        Map<String, String> snapPropsFromSource = data.getMetaData().getRsc().getSnapProps();
        snapPropsFromSource.put(InternalApiConsts.KEY_BACKUP_L2L_SRC_CLUSTER_UUID, data.getSrcClusterId());
        snapPropsFromSource.put(
            InternalApiConsts.KEY_BACKUP_L2L_SRC_CLUSTER_SHORT_HASH,
            clusterHash == null ? "" : clusterHash
        );
        ctrlTransactionHelper.commit();

        return ctrlSatelliteUpdateCaller.updateSatellites(stltRemote)
            .thenMany(
                freeCapacityFetcher.fetchThinFreeCapacities(
                    data.getDstNodeName() == null ?
                        Collections.emptySet() :
                        Collections.singleton(LinstorParsingUtils.asNodeName(data.getDstNodeName()))
                ).flatMapMany(
                    ignoredThinFreeCapacities -> scopeRunner.fluxInTransactionalScope(
                        "restore backup",
                        lockGuardFactory.buildDeferred(
                            LockType.WRITE,
                            LockObj.NODES_MAP,
                            LockObj.RSC_DFN_MAP,
                            // restoreBackupL2LInTransaction can move the resource definition into the target
                            // resource group (force-move-resource-group), which mutates ResourceGroup objects.
                            // Lock RSC_GRP_MAP to serialize against resource-group create/modify/delete.
                            LockObj.RSC_GRP_MAP
                        ),
                        () -> backupRestoreApiCallHandler.restoreBackupL2LInTransaction(
                            data
                        )
                    )
                )
        );
    }

    private @Nullable String getSrcClusterShortId(String srcClusterIdRef)
    {
        @Nullable String ret;
        try
        {
            Props props = systemConfRepository.getCtrlConfForChange();
            @Nullable String localClusterId = props.getProp(
                InternalApiConsts.KEY_CLUSTER_LOCAL_ID,
                ApiConsts.NAMESPC_CLUSTER
            );
            if (Objects.equals(localClusterId, srcClusterIdRef))
            {
                // we are shipping to ourself. do not return a clusterShortID.
                ret = null;
            }
            else
            {
                ret = props.getProp(srcClusterIdRef, ApiConsts.NAMESPC_CLUSTER_REMOTE);
                if (ret == null)
                {
                    ret = generateClusterShortId();

                    @Nullable Props namespace = props.getNamespace(ApiConsts.NAMESPC_CLUSTER_REMOTE);
                    if (namespace != null)
                    {
                        HashSet<String> existingHashes = new HashSet<>(namespace.values());

                        while (existingHashes.contains(ret))
                        {
                            ret = generateClusterShortId();
                        }
                    }

                    props.setProp(srcClusterIdRef, ret, ApiConsts.NAMESPC_CLUSTER_REMOTE);
                }
            }
        }
        catch (InvalidKeyException | DatabaseException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    public String generateClusterShortId()
    {
        final SecretGenerator secretGen = cryptoProvider.createSecretGenerator();
        return new BigInteger(secretGen.generateSecret(5)).abs().toString(36);
    }

    private BackupShippingResponse snapshotToResponse(
        BackupShippingStartInfo startInfo
    )
    {
        BackupShippingResponse ret;
        ApiCallRcImpl responses = new ApiCallRcImpl();
        Snapshot snap = startInfo.apiCallRcWithSnapshot.extractApiCallRc(responses);
        if (snap.isDeleted())
        {
            responses.addEntry(
                "Error while trying to start receiving, Snapshot has already been deleted due to it.",
                ApiConsts.FAIL_UNKNOWN_ERROR
            );
            ret = new BackupShippingResponse(
                false,
                responses
            );
        }
        else
        {
            ret = new BackupShippingResponse(
                true,
                responses
            );
        }
        return ret;
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
        Flux<ApiCallRc> flux;
        ApiCallRcImpl responses = new ApiCallRcImpl();
        try
        {
            NetInterface netIf = null;
            AbsRemote remote = remoteRepo.get(new RemoteName(remoteName, true));
            Map<String, Integer> ports;
            if (remote instanceof StltRemote stltRemote)
            {
                ports = stltRemote.getPorts();
            }
            else
            {
                throw new ImplementationError(
                    "Expected type StltRemote, instead got " + remote.getClass().getCanonicalName()
                );
            }
            Snapshot snap = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapName)
                .getSnapshot(new NodeName(nodeName));
            BackupShippingDstData data = backupInfoMgr.getL2LDstData(snap);
            Node node = snap.getNode();
            if (data.getDstNetIfName() != null)
            {
                netIf = node.getNetInterface(
                    LinstorParsingUtils.asNetInterfaceName(data.getDstNetIfName())
                );
            }
            if (netIf == null)
            {
                netIf = node.iterateNetInterfaces().next();
            }


            String srcStltRemoteName;
            synchronized (dstToSrcStltRemoteNameMap)
            {
                srcStltRemoteName = dstToSrcStltRemoteNameMap.remove(data.getStltRemote().getName());
            }

            boolean useZstd = data.getStltRemote().useZstd();

            flux = backupShippingRestClient.sendBackupReceiveRequest(
                new BackupShippingReceiveRequest(
                    true,
                    responses,
                    data.getSrcL2LRemoteName(),
                    data.getStltRemote().getName().displayValue,
                    data.getSrcL2LRemoteUrl(),
                    netIf.getAddress().getAddress(),
                    ports,
                    data.getIncrBaseSnapDfnUuid(),
                    useZstd,
                    srcStltRemoteName
                )
            ).thenMany(Flux.empty()); // request is from stlt, so instead of converting from JsonGenTypes.ApiCallRc to
                                      // ApiCallRc the response gets ignored
        }
        catch (InvalidNameException exc)
        {
            errorReporter.reportError(exc);
            responses = ApiCallRcImpl.singleApiCallRc(
                ApiConsts.FAIL_INVLD_NODE_NAME,
                "Invalid node name given",
                exc.getMessage()
            );
            flux = Flux.just(responses);
        }
        return flux;
    }

    public Flux<ApiCallRc> prepareForAbort(
        BackupShippingPrepareAbortRequest data
    )
    {
        Flux<ApiCallRc> flux;
        LinstorRemote otherRemote = loadLinstorRemote(data.clusterId);
        ApiCallRcImpl rc = isClusterAllowedContact(data.clusterVersion, data.clusterId, otherRemote);
        if (rc != null)
        {
            flux = Flux.just(rc);
        }
        else
        {
            flux = scopeRunner.fluxInTransactionalScope(
                "pepare for abort",
                lockGuardFactory.create()
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> prepareForAbortInTransaction(data)
            );
        }
        return flux;
    }

    private Flux<ApiCallRc> prepareForAbortInTransaction(BackupShippingPrepareAbortRequest data)
    {
        final String otherClusterHash = "_" + getSrcClusterShortId(data.clusterId);
        Set<SnapshotDefinition> snapsToUpdate = new HashSet<>();
        for (Entry<String, List<String>> rscAndSnapNames : data.rscAndSnapNamesToAbort.entrySet())
        {
            String rscName = rscAndSnapNames.getKey();
            for (String snapName : rscAndSnapNames.getValue())
            {
                String actualSnapName;
                String shippingStatus;
                if (snapName.endsWith(otherClusterHash))
                {
                    // we are src-cluster, target sent us snap-name + hash
                    actualSnapName = snapName.substring(0, snapName.length() - otherClusterHash.length());
                    // on the source the in-progress shipment is actually aborted separately (setFlagsAndAbort),
                    // so here the prepare-abort is only a heads-up that lets the sending satellite treat the
                    // upcoming connection drop as an abort instead of an error.
                    shippingStatus = InternalApiConsts.VALUE_PREPARE_ABORT;
                }
                else
                {
                    // we are target cluster, src sent us snap-name, but we need snap-name + hash
                    actualSnapName = snapName + otherClusterHash;
                    // VALUE_ABORTING (instead of the merely-passive VALUE_PREPARE_ABORT) makes the receiving
                    // satellite actively tear down the running receive-daemon and send BackupShippingReceived,
                    // which triggers shippingReceived() and releases the restore-lock (the BackupInfoManager
                    // restore-entries and the RESTORE_TARGET flag) as well as deleting the partially received
                    // snapshot. With VALUE_PREPARE_ABORT the satellite only marks the daemon and waits for the
                    // sender's connection to drop on its own; if that never cleanly happens (e.g. the source
                    // side is torn down while the receive is throttled) the notification never arrives,
                    // restoreContainsRscDfn() stays true forever and any further restore/delete of this
                    // resource is rejected with "is currently being restored from a backup" until the
                    // controller is restarted.
                    shippingStatus = InternalApiConsts.VALUE_ABORTING;
                }
                // no need to parse string into SnapshotName since the snap exists on other cluster with that name
                @Nullable SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfnOrNull(rscName, actualSnapName);
                if (snapDfn != null)
                {
                    try
                    {
                        snapDfn.getSnapDfnProps()
                            .setProp(
                                InternalApiConsts.KEY_SHIPPING_STATUS,
                                shippingStatus,
                                BackupShippingUtils.BACKUP_TARGET_PROPS_NAMESPC
                            );
                        snapsToUpdate.add(snapDfn);
                    }
                    catch (InvalidKeyException | InvalidValueException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                    catch (DatabaseException exc)
                    {
                        throw new ApiDatabaseException(exc);
                    }
                }
            }
        }
        ctrlTransactionHelper.commit();
        Flux<ApiCallRc> flux = Flux.empty();
        for (SnapshotDefinition snapDfn : snapsToUpdate)
        {
            flux = flux.concatWith(
                ctrlSatelliteUpdateCaller.updateSatellites(
                    snapDfn,
                    CtrlSatelliteUpdateCaller.notConnectedWarn()
                )
                    .transform(
                        resp -> CtrlResponseUtils.combineResponses(
                            errorReporter,
                            resp,
                            snapDfn.getResourceName(),
                            "Aborting backup ''" + snapDfn.getName() + "'' of {1} on {0}"
                        )
                    )
            );
        }
        return flux;
    }

    static class BackupShippingStartInfo
    {
        private final ApiCallRcWith<Snapshot> apiCallRcWithSnapshot;

        BackupShippingStartInfo(ApiCallRcWith<Snapshot> apiCallRcWithSnapshotRef)
        {
            apiCallRcWithSnapshot = apiCallRcWithSnapshotRef;
        }
    }

}
