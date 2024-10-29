package com.linbit.linstor.core.apicallhandler.controller.backup;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.BackupToS3;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.backups.BackupInfoVlmPojo;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.api.pojo.backups.BackupMetaInfoPojo;
import com.linbit.linstor.api.pojo.backups.LuksLayerMetaPojo;
import com.linbit.linstor.api.pojo.backups.VlmDfnMetaPojo;
import com.linbit.linstor.api.pojo.backups.VlmMetaPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.backupshipping.S3MetafileNameInfo;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRemoteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotRestoreApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LDstApiCallHandler.BackupShippingStartInfo;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingRestClient;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingDstData;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingReceiveDoneRequest;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.MissingKeyPropertyException;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition.Flags;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerSizeHelper;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.utils.PropsUtils;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.MathUtils;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.backupshipping.BackupConsts.META_SUFFIX;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlBackupRestoreApiCallHandler
{
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final Provider<AccessContext> peerAccCtx;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSnapshotCrtApiCallHandler snapshotCrtHandler;
    private final BackupToS3 backupHandler;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlBackupApiHelper backupHelper;
    private final AccessContext sysCtx;
    private final ErrorReporter errorReporter;
    private final EncryptionHelper encHelper;
    private final DecryptionHelper decHelper;
    private final CtrlSnapshotCrtHelper snapshotCrtHelper;
    private final CtrlVlmDfnCrtApiHelper ctrlVlmDfnCrtApiHelper;
    private final CtrlRscDfnApiCallHandler ctrlRscDfnApiCallHandler;
    private final Autoplacer autoplacer;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final RemoteRepository remoteRepo;
    private final Provider<Peer> peerProvider;
    private final DynamicNumberPool snapshotShippingPortPool;
    private final CtrlSnapshotRestoreApiCallHandler ctrlSnapRestoreApiCallHandler;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteApiCallHandler;
    private final CtrlBackupApiCallHandler backupApiCallHandler;
    private final BackupShippingRestClient backupShippingRestClient;
    private final LayerSizeHelper layerSizeHelper;
    private final SystemConfRepository systemConfRepository;
    private final CtrlRscDeleteApiCallHandler ctrlRscDelApiCallHandler;
    private final CtrlRemoteApiCallHandler ctrlRemoteApiCallHandler;

    @Inject
    public CtrlBackupRestoreApiCallHandler(
        FreeCapacityFetcher freeCapacityFetcherRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSnapshotCrtApiCallHandler snapshotCrtHandlerRef,
        BackupToS3 backupHandlerRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlBackupApiHelper backupHelperRef,
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        EncryptionHelper encHelperRef,
        DecryptionHelper decHelperRef,
        CtrlSnapshotCrtHelper snapCrtHelperRef,
        CtrlRscDfnApiCallHandler ctrlRscDfnApiCallHandlerRef,
        CtrlVlmDfnCrtApiHelper ctrlVlmDfnCrtApiHelperRef,
        Autoplacer autoplacerRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        RemoteRepository remoteRepoRef,
        Provider<Peer> peerProviderRef,
        @Named(NumberPoolModule.SNAPSHOPT_SHIPPING_PORT_POOL) DynamicNumberPool snapshotShippingPortPoolRef,
        CtrlSnapshotRestoreApiCallHandler ctrlSnapRestoreApiCallHandlerRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteApiCallHandlerRef,
        CtrlBackupApiCallHandler backupApiCallHandlerRef,
        BackupShippingRestClient backupShippingRestClientRef,
        LayerSizeHelper layerSizeHelperRef,
        SystemConfRepository systemConfRepositoryRef,
        CtrlRscDeleteApiCallHandler ctrlRscDelApiCallHandlerRef,
        CtrlRemoteApiCallHandler ctrlRemoteApiCallHandlerRef
    )
    {
        freeCapacityFetcher = freeCapacityFetcherRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        peerAccCtx = peerAccCtxRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        snapshotCrtHandler = snapshotCrtHandlerRef;
        backupHandler = backupHandlerRef;
        backupInfoMgr = backupInfoMgrRef;
        backupHelper = backupHelperRef;
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        encHelper = encHelperRef;
        decHelper = decHelperRef;
        snapshotCrtHelper = snapCrtHelperRef;
        ctrlRscDfnApiCallHandler = ctrlRscDfnApiCallHandlerRef;
        ctrlVlmDfnCrtApiHelper = ctrlVlmDfnCrtApiHelperRef;
        autoplacer = autoplacerRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        remoteRepo = remoteRepoRef;
        peerProvider = peerProviderRef;
        snapshotShippingPortPool = snapshotShippingPortPoolRef;
        ctrlSnapRestoreApiCallHandler = ctrlSnapRestoreApiCallHandlerRef;
        ctrlSnapDeleteApiCallHandler = ctrlSnapDeleteApiCallHandlerRef;
        backupApiCallHandler = backupApiCallHandlerRef;
        backupShippingRestClient = backupShippingRestClientRef;
        layerSizeHelper = layerSizeHelperRef;
        systemConfRepository = systemConfRepositoryRef;
        ctrlRscDelApiCallHandler = ctrlRscDelApiCallHandlerRef;
        ctrlRemoteApiCallHandler = ctrlRemoteApiCallHandlerRef;
    }

    public Flux<ApiCallRc> restoreBackup(
        String srcRscName,
        String srcSnapName,
        String backupId,
        Map<String, String> storPoolMapRef,
        String nodeName,
        String targetRscName,
        @Nullable String dstRscGrpRef,
        String remoteName,
        String passphrase,
        boolean downloadOnly,
        boolean forceRestore,
        boolean forceRscGrpRef
    )
    {
        return freeCapacityFetcher
            .fetchThinFreeCapacities(
                Collections.singleton(LinstorParsingUtils.asNodeName(nodeName))
            ).flatMapMany(
                ignored -> scopeRunner.fluxInTransactionalScope(
                    "restore backup", lockGuardFactory.buildDeferred(
                        LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP
                    ),
                    () -> restoreBackupInTransaction(
                        srcRscName,
                        srcSnapName,
                        backupId,
                        storPoolMapRef,
                        nodeName,
                        targetRscName,
                        dstRscGrpRef,
                        remoteName,
                        passphrase,
                        downloadOnly,
                        forceRestore,
                        forceRscGrpRef
                    )
                )
            );
    }

    /**
     * Restores a backup.<br/>
     * More detailed order of things:
     * <ul>
     * <li>Makes sure the given node can is reachable</li>
     * <li>Finds the correct backup(s) to restore</li>
     * <li>Checks that there is enough space in the storPool(s) to receive & restore the backups</li>
     * <li>Creates all needed Snapshots</li>
     * <li>Sets all props on the first snap to start receiving</li>
     * </ul>
     */
    private Flux<ApiCallRc> restoreBackupInTransaction(
        String srcRscName,
        String srcSnapName,
        String backupId,
        Map<String, String> storPoolMap,
        String nodeNameStrRef,
        String targetRscName,
        @Nullable String dstRscGrpRef,
        String remoteName,
        String passphrase,
        boolean downloadOnly,
        boolean forceRestore,
        boolean forceRscGrpRef
    ) throws AccessDeniedException, InvalidNameException
    {
        // 1. Ensure node is ready
        String nodeNameStr = nodeNameStrRef;
        Node node = ctrlApiDataLoader.loadNode(nodeNameStr, true);
        if (!node.getPeer(peerAccCtx.get()).isOnline())
        {
            throw new ApiRcException(
                ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_CONNECTED,
                        "No active connection to satellite '" + node.getName() + "'."
                    )
                    .setDetails("Backups cannot be restored when the corresponding satellite is not connected.")
                    .build()
            );
        }

        // 2. Select backup to restore from
        S3Remote remote = backupHelper.getS3Remote(remoteName);
        byte[] targetMasterKey = backupHelper.getLocalMasterKey();
        S3MetafileNameInfo toRestore;
        Set<String> s3keys;
        List<S3ObjectSummary> objects;

        if (backupId != null && !backupId.isEmpty())
        {
            // We have an explicit backup that should be restored
            String metaName = backupId;
            if (!metaName.endsWith(META_SUFFIX))
            {
                metaName = backupId + META_SUFFIX;
            }
            try
            {
                toRestore = new S3MetafileNameInfo(metaName);
                objects = backupHandler.listObjects(toRestore.rscName, remote, peerAccCtx.get(), targetMasterKey);
                // do not use backupHelper.getAllS3Keys here to avoid two listObjects calls since objects is needed
                // later
                s3keys = objects.stream().map(S3ObjectSummary::getKey).collect(Collectors.toCollection(TreeSet::new));
            }
            catch (ParseException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                        "The target backup " + backupId + " is invalid since it does not match the pattern of " +
                            "'<rscName>_back_YYYYMMDD_HHMMSS[optional-backup-s3-suffix][^snapshot-name][.meta] " +
                            "(e.g. my-rsc_back_20210824_072543)'. " +
                            "Please provide a valid target backup, or provide only the source resource name to " +
                            "restore to the latest backup of that resource."
                    )
                );
            }
        }
        else
        {
            // No backup was explicitly selected, use the latest available for the source resource.
            objects = backupHandler.listObjects(srcRscName, remote, peerAccCtx.get(), targetMasterKey);
            // do not use backupHelper.getAllS3Keys here to avoid two listObjects calls since objects is needed later
            s3keys = objects.stream().map(S3ObjectSummary::getKey).collect(Collectors.toCollection(TreeSet::new));
            toRestore = backupHelper.getLatestBackup(s3keys, srcSnapName);
        }

        // Sanity check, now we should have a metafile, and it should be contained in the s3keys
        if (toRestore == null || !s3keys.contains(toRestore.toString()))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                    "The target backup " + toRestore + " is invalid since it does not exist in the given remote " +
                        remoteName + ". " +
                        "Please provide a valid target backup, or provide only the source resource name to " +
                        "restore to the latest backup of that resource."
                )
            );
        }

        if (backupInfoMgr.restoreContainsMetaFile(toRestore.toString()))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_IN_USE | ApiConsts.MASK_BACKUP,
                    "The meta-file " + toRestore + " is currently being used in a restore."
                )
            );
        }

        // 3. Create snapshot objects, starting with the found backup, and adding every incremental backup.
        // In order to restore that, we need to start with the full backup, continue with the first, second ,... and
        // finally the last incremental backup.
        try
        {
            Snapshot nextBackup = null;
            // reset data to props of final snapshot
            boolean resetData = !downloadOnly && !hasTargetRscDfnResources(targetRscName);
            S3MetafileNameInfo currentMetaFile = toRestore;
            Map<StorPoolApi, List<BackupInfoVlmPojo>> storPoolInfo = new HashMap<>();
            List<Pair<S3MetafileNameInfo, BackupMetaDataPojo>> metadataChain = new ArrayList<>();

            // 3a. Follow the metadata chain, adding all to metadataChain until we reach a full backup.
            do
            {
                BackupMetaDataPojo metadata = backupHandler.getMetaFile(
                    currentMetaFile.toString(),
                    remote,
                    peerAccCtx.get(),
                    targetMasterKey
                );
                metadataChain.add(new Pair<>(currentMetaFile, metadata));
                String base = metadata.getBasedOn();
                if (base != null && !base.isEmpty())
                {
                    currentMetaFile = new S3MetafileNameInfo(base);
                }
                else
                {
                    currentMetaFile = null;
                }
            }
            while (currentMetaFile != null);

            @Nullable ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(targetRscName, false);
            if (rscDfn != null)
            {
                NodeName nodeName = node.getName();
                int idx = 0;
                boolean stop = false;
                // This loop finds the first snapshot that has already been downloaded during a previous restore.
                // In case that download happened on a different node, that node will be used instead.
                for (Pair<S3MetafileNameInfo, BackupMetaDataPojo> metadata : metadataChain)
                {
                    @Nullable SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfn(
                        rscDfn,
                        new SnapshotName(metadata.objA.snapName),
                        false
                    );
                    if (snapDfn != null)
                    {
                        @Nullable Snapshot snap = snapDfn.getSnapshot(peerAccCtx.get(), nodeName);
                        Collection<Snapshot> allSnaps = snapDfn.getAllSnapshots(peerAccCtx.get());
                        if (snap != null)
                        {
                            // this snap already exists, we don't need the chain from this point on
                            stop = true;
                        }
                        else if (!allSnaps.isEmpty())
                        {
                            // ensure we are downloading on a node that has the snapshot
                            node = allSnaps.iterator().next().getNode();
                            nodeNameStr = node.getName().displayValue;
                            stop = true;
                        }
                        else
                        {
                            throw new ImplementationError("Empty snapDfn " + snapDfn + " should not exists.");
                        }
                    }
                    if (stop)
                    {
                        break;
                    }
                    ++idx;
                }
                // the backup on idx already exists, therefore it has to contain the data of all backups before it,
                // which means we don't need to download any backups in the chain after this point
                // this can result in an empty list
                metadataChain = metadataChain.subList(0, idx);
            }

            // 3b. check if given storpools have enough space remaining for the restore
            boolean first = true;
            for (Pair<S3MetafileNameInfo, BackupMetaDataPojo> meta : metadataChain)
            {
                Pair<Long, Long> totalSizes = new Pair<>(0L, 0L); // dlSize, allocSize
                backupApiCallHandler
                    .fillBackupInfo(first, storPoolInfo, objects, meta.objB, meta.objB.getLayerData(), totalSizes);
                first = false;
            }
            Map<String, Long> remainingFreeSpace = backupApiCallHandler
                .getRemainingSize(storPoolInfo, storPoolMap, nodeNameStr);
            List<String> spTooFull = new ArrayList<>();
            for (Entry<String, Long> spaceEntry : remainingFreeSpace.entrySet())
            {
                if (spaceEntry != null && spaceEntry.getValue() < 0)
                {
                    spTooFull.add(
                        ctrlApiDataLoader.loadStorPool(spaceEntry.getKey(), nodeNameStr, true).getName().displayValue
                    );
                }
            }

            if (!spTooFull.isEmpty())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_ENOUGH_FREE_SPACE | ApiConsts.MASK_BACKUP,
                        "The storage-pool(s) " + StringUtils.join(spTooFull, ", ") + " do(es) not have enough space " +
                            "remaining for the restore." +
                            " For more information, please use the 'backup info' command."
                    )
                );
            }

            // 3c. Create snapshot objects for backups, setting the restore property to the source metadata file
            ApiCallRcImpl responses = new ApiCallRcImpl();
            List<Snapshot> allSnaps = new ArrayList<>();
            Map<Snapshot, Snapshot> restoreOrder = new TreeMap<>();
            for (Pair<S3MetafileNameInfo, BackupMetaDataPojo> metadata : metadataChain)
            {
                Snapshot snap = createSnapshotByS3Meta(
                    metadata.objA,
                    storPoolMap,
                    node,
                    targetRscName,
                    dstRscGrpRef,
                    passphrase,
                    remote,
                    s3keys,
                    metadata.objB,
                    responses,
                    resetData,
                    downloadOnly,
                    forceRestore,
                    forceRscGrpRef
                );
                allSnaps.add(snap);
                // all other "basedOn" snapshots should not change props / size / etc..
                resetData = false;
                snap.getSnapProps(peerAccCtx.get())
                    .setProp(
                        InternalApiConsts.KEY_BACKUP_TO_RESTORE,
                        metadata.objA.toString(),
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );
                if (nextBackup != null)
                {
                    restoreOrder.put(snap, nextBackup);
                }
                nextBackup = snap;
            }

            if (nextBackup == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_EXISTS_SNAPSHOT | ApiConsts.MASK_BACKUP,
                        "The requested backup has already been downloaded to this cluster. " +
                            "Please use 'snapshot resource restore' to get a resource.",
                        true
                    )
                );
            }
            // 3d. nextBackup now points to the end of the chain, i.e. the full backup. Start restoring from here
            nextBackup.getFlags().enableFlags(peerAccCtx.get(), Snapshot.Flags.BACKUP_TARGET);
            if (!backupInfoMgr.addAllRestoreEntries(
                nextBackup.getResourceDefinition(),
                toRestore.toString(),
                targetRscName,
                allSnaps,
                restoreOrder,
                remote.getName()
                )
            )
            {
                throw new ImplementationError(
                    "Tried to overwrite existing backup-info-mgr entry for rscDfn " + targetRscName
                );
            }
            ctrlTransactionHelper.commit();
            responses.addEntry(
                "Restoring backup of resource " + toRestore.rscName + " from remote " + remoteName +
                    " into resource " + targetRscName + " in progress.",
                ApiConsts.MASK_INFO
            );
            SnapshotDefinition nextBackSnapDfn = nextBackup.getSnapshotDefinition();
            return snapshotCrtHandler.postCreateSnapshot(nextBackSnapDfn, false)
                .concatWith(Flux.just(responses))
                .onErrorResume(
                    error -> cleanupAfterFailedRestore(error, nextBackSnapDfn, targetRscName)
                );
        }
        catch (IOException | ParseException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                    "Failed to parse meta file " + toRestore.toString()
                )
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "restore backup",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN | ApiConsts.MASK_BACKUP
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
    }

    private boolean hasTargetRscDfnResources(String targetRscNameRef)
    {
        boolean ret = false;
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(targetRscNameRef, false);
        if (rscDfn != null)
        {
            ret = rscDfn.getResourceCount() > 0;
        }
        return ret;
    }

    /**
     * Create snaps and snapDfns based on the info from the meta-file<br/>
     * Also re-encrypt LUKS-vlm-keys if needed
     */
    private Snapshot createSnapshotByS3Meta(
        S3MetafileNameInfo metafileNameInfo,
        Map<String, String> storPoolMap,
        Node node,
        String targetRscName,
        @Nullable String dstRscGrpRef,
        String passphrase,
        S3Remote remote,
        Set<String> s3keys,
        BackupMetaDataPojo metadata,
        ApiCallRcImpl responses,
        boolean resetData,
        boolean downloadOnly,
        boolean forceRestore,
        boolean forceRscGrpRef
    )
        throws AccessDeniedException, ImplementationError, DatabaseException
    {
        // 1. Ensure we have all the required files
        for (List<BackupMetaInfoPojo> backupList : metadata.getBackups().values())
        {
            for (BackupMetaInfoPojo backup : backupList)
            {
                if (!s3keys.contains(backup.getName()))
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_NOT_FOUND_SNAPSHOT | ApiConsts.MASK_BACKUP,
                            "Failed to find backup " + backup.getName()
                        )
                    );
                }
            }
        }

        // 2. Ensure we have the required information to restore LUKS volumes
        LuksLayerMetaPojo luksInfo = metadata.getLuksInfo();
        byte[] srcMasterKey = null;
        if (luksInfo != null)
        {
            if (passphrase == null || passphrase.isEmpty())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY | ApiConsts.MASK_BACKUP,
                        "The resource " + metafileNameInfo.rscName +
                            " to be restored seems to have luks configured, but no passphrase was given."
                    )
                );
            }
            try
            {
                srcMasterKey = encHelper.getDecryptedMasterKey(
                    luksInfo.getMasterCryptHash(),
                    luksInfo.getMasterPassword(),
                    luksInfo.getMasterCryptSalt(),
                    passphrase
                );
            }
            catch (MissingKeyPropertyException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                        "Some of the needed properties were not set in the metadata-file " + metafileNameInfo +
                            ". The metadata-file is probably corrupted and therefore unusable."
                    ),
                    exc
                );
            }
            catch (LinStorException exc)
            {
                errorReporter.reportError(exc);
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                        "Decrypting the master password failed."
                    )
                );
            }
        }

        // 3. Create definitions based on metadata
        SnapshotName snapName = LinstorParsingUtils.asSnapshotName(metafileNameInfo.snapName);
        ResourceDefinition rscDfn = getRscDfnForBackupRestore(
            targetRscName,
            snapName,
            metadata,
            resetData,
            dstRscGrpRef,
            forceRscGrpRef,
            responses
        );

        SnapshotDefinition snapDfn = getSnapDfnForBackupRestore(
            metadata,
            snapName,
            rscDfn,
            responses,
            remote,
            downloadOnly,
            forceRestore
        );
        Map<Integer, SnapshotVolumeDefinition> snapVlmDfns = new TreeMap<>();
        createSnapVlmDfnForBackupRestore(
            targetRscName,
            metadata,
            rscDfn,
            snapDfn,
            snapVlmDfns,
            resetData
        );

        // 4. Create snapshots
        Snapshot snap = createSnapshotAndVolumesForBackupRestore(
            metadata,
            metadata.getLayerData(),
            node,
            snapDfn,
            snapVlmDfns,
            storPoolMap,
            remote,
            responses
        );

        // 5. re-encrypt LUKS keys if needed
        if (srcMasterKey != null)
        {
            List<AbsRscLayerObject<Snapshot>> luksLayers = LayerUtils.getChildLayerDataByKind(
                snap.getLayerData(peerAccCtx.get()),
                DeviceLayerKind.LUKS
            );
            try
            {
                for (AbsRscLayerObject<Snapshot> layer : luksLayers)
                {
                    for (VlmProviderObject<Snapshot> vlm : layer.getVlmLayerObjects().values())
                    {
                        LuksVlmData<Snapshot> luksVlm = (LuksVlmData<Snapshot>) vlm;
                        byte[] vlmKey = luksVlm.getEncryptedKey();
                        byte[] decryptedKey = decHelper.decrypt(srcMasterKey, vlmKey);

                        byte[] encVlmKey = encHelper.encrypt(decryptedKey);
                        luksVlm.setEncryptedKey(encVlmKey);
                    }
                }
            }
            catch (LinStorException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                        "De- or encrypting the volume passwords failed."
                    ),
                    exc
                );
            }
        }

        return snap;
    }

    private Flux<ApiCallRc> cleanupAfterFailedRestore(
        Throwable throwable,
        SnapshotDefinition snapDfnRef,
        String rscName
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "cleanup after failed restore", lockGuardFactory.buildDeferred(
                LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP
            ),
            () -> cleanupAfterFailedRestoreInTransaction(
                throwable,
                snapDfnRef,
                rscName
            )
        );
    }

    /**
     * Remove all restore-references of the given snapDfn from the backupInfoMgr
     */
    private Flux<ApiCallRc> cleanupAfterFailedRestoreInTransaction(
        Throwable throwableRef,
        SnapshotDefinition snapDfnRef,
        String rscName
    ) throws AccessDeniedException
    {
        Flux<ApiCallRc> flux = Flux.empty();
        for (Snapshot snap : snapDfnRef.getAllSnapshots(sysCtx))
        {
            flux = flux.concatWith(
                ctrlRemoteApiCallHandler.cleanupRemotesIfNeeded(
                    backupInfoMgr.removeAllRestoreEntries(
                        snapDfnRef.getResourceDefinition(),
                        rscName,
                        snap
                    )
                )
            );
        }

        return flux.concatWith(Flux.error(throwableRef));
    }

    /**
     * Creates snapshot and snapVlms based on the given metadata
     */
    private Snapshot createSnapshotAndVolumesForBackupRestore(
        BackupMetaDataPojo metadata,
        RscLayerDataApi layers,
        Node node,
        SnapshotDefinition snapDfn,
        Map<Integer, SnapshotVolumeDefinition> snapVlmDfns,
        Map<String, String> renameMap,
        AbsRemote remote,
        @Nullable ApiCallRc apiCallRc
    )
        throws AccessDeniedException, DatabaseException
    {
        Snapshot snap = snapshotCrtHelper
            .restoreSnapshot(snapDfn, node, layers, renameMap, apiCallRc);
        AccessContext accCtx = peerAccCtx.get();
        Props snapProps = snap.getSnapProps(accCtx);

        PropsUtils.resetProps(metadata.getRsc().getRscProps(), snap.getRscPropsForChange(accCtx));
        PropsUtils.resetProps(metadata.getRsc().getSnapProps(), snapProps);
        try
        {
            snapProps.setProp(
                InternalApiConsts.KEY_BACKUP_SRC_REMOTE,
                remote.getName().displayValue,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );

            List<DeviceLayerKind> usedDeviceLayerKinds = LayerUtils.getUsedDeviceLayerKinds(
                snap.getLayerData(accCtx),
                accCtx
            );
            usedDeviceLayerKinds.removeAll(
                node.getPeer(accCtx)
                    .getExtToolsManager().getSupportedLayers()
            );
            if (!usedDeviceLayerKinds.isEmpty())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_LAYER_STACK | ApiConsts.MASK_BACKUP,
                        "The node does not support the following needed layers: " + usedDeviceLayerKinds.toString()
                    )
                );
            }

            for (Entry<Integer, VlmMetaPojo> vlmMetaEntry : metadata.getRsc().getVlms().entrySet())
            {
                SnapshotVolume snapVlm = snapshotCrtHelper
                    .restoreSnapshotVolume(layers, snap, snapVlmDfns.get(vlmMetaEntry.getKey()), renameMap, apiCallRc);

                VlmMetaPojo vlmMetaPojo = metadata.getRsc().getVlms().get(vlmMetaEntry.getKey());
                PropsUtils.resetProps(vlmMetaPojo.getVlmProps(), snapVlm.getVlmPropsForChange(accCtx));
                PropsUtils.resetProps(vlmMetaPojo.getSnapVlmProps(), snapVlm.getSnapVlmProps(accCtx));

                recalculateCommonAllocationGranularityIfNeeded(snapVlm);
            }
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        return snap;
    }

    /**
     * <p>
     * Pre version 1.26.0 (where storpool-mixing was introduced), backups had no
     * "StorDriver/internal/AllocationGranularity" property on VlmDfn.
     * </p>
     * <p>
     * If those backups had a non-default extent-size, new peers (additional to the one that we might restore shortly)
     * will create themselves with a possibly too small disk, leading to Standalone scenario on DRBD level.
     * If the property is missing, we cannot reconstruct the exact allocation-granularity here, but we can calculate a
     * "good enough" estimate of it, so that new peers will not be able to create too small devices. This is done with
     * the following calculation:
     * </p>
     * <p>
     * Let vds be the size from the volumeDefinition, bds the usable size of the snapshot (STORAGE layer of the
     * snapVlm).
     * </p>
     * <p>
     * From vds we need to calculate the additional sizes for metadata (DRBD, LUKS, etc..), which gives us the minimum
     * usable size on STORAGE layer if we would create the resource now as it is. Since we have the minimum usable size
     * as well as the actual usable size of the snapshot (bds), we can calculate a granularity G. G is expected to be a
     * value that, when applied in a new calculation of the usable size for the STORAGE layer, the new usable size would
     * be equal or greater than the previous minimum size as well as equal or greater than bds. This new usable size
     * would ensure that new peers would rather be a bit too large than too small, which is usually fine for DRBD
     * setups.
     * </p>
     * <p>
     * That G value needs to be stored on the SnapshotVolumeDefinition's property for future usage.
     * </p>
     *
     * @param snapVlmRef
     *
     * @throws AccessDeniedException
     * @throws InvalidValueException
     * @throws DatabaseException
     * @throws InvalidKeyException
     */
    private void recalculateCommonAllocationGranularityIfNeeded(SnapshotVolume snapVlmRef)
        throws AccessDeniedException, InvalidKeyException, DatabaseException, InvalidValueException
    {
        SnapshotVolumeDefinition snapVlmDfn = snapVlmRef.getSnapshotVolumeDefinition();
        Props snapVlmDfnProps = snapVlmDfn.getVlmDfnPropsForChange(sysCtx);
        @Nullable String allocGranPropValue = snapVlmDfnProps.getProp(
            InternalApiConsts.ALLOCATION_GRANULARITY,
            StorageConstants.NAMESPACE_INTERNAL
        );
        if (allocGranPropValue == null)
        {
            Set<AbsRscLayerObject<Snapshot>> snapStorageVlmSet = LayerRscUtils.getRscDataByLayer(
                snapVlmRef.getAbsResource().getLayerData(sysCtx),
                DeviceLayerKind.STORAGE,
                RscLayerSuffixes.SUFFIX_DATA::equals
            );
            if (snapStorageVlmSet.size() == 1)
            {
                long bds = snapStorageVlmSet.iterator().next()
                    .getVlmProviderObject(snapVlmDfn.getVolumeNumber())
                    .getUsableSize();
                long minimumSize = layerSizeHelper.calculateSize(sysCtx, snapVlmRef, RscLayerSuffixes.SUFFIX_DATA);
                long diff = (bds > minimumSize ? bds - minimumSize : minimumSize - bds);
                // make sure recalcAllocGran is always > 1
                long recalcAllocGran = diff > 1 ? MathUtils.longCeilingPowerTwo(diff) : 1;

                snapVlmDfnProps.setProp(InternalApiConsts.ALLOCATION_GRANULARITY,
                    Long.toString(recalcAllocGran),
                    StorageConstants.NAMESPACE_INTERNAL
                );
            }
        }
    }

    /**
     * Creates the snapVlmDfn based on the given metadata.
     * Also resets flags if resetData is true
     */
    private long createSnapVlmDfnForBackupRestore(
        String targetRscName,
        BackupMetaDataPojo metadata,
        ResourceDefinition rscDfn,
        SnapshotDefinition snapDfn,
        Map<Integer, SnapshotVolumeDefinition> snapVlmDfns,
        boolean resetData
    )
        throws AccessDeniedException, DatabaseException
    {
        long totalSize = 0;
        for (Entry<Integer, VlmDfnMetaPojo> vlmDfnMetaEntry : metadata.getRscDfn().getVlmDfns().entrySet())
        {
            VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfn(targetRscName, vlmDfnMetaEntry.getKey(), false);
            if (vlmDfn == null)
            {
                vlmDfn = ctrlVlmDfnCrtApiHelper.createVlmDfnData(
                    peerAccCtx.get(),
                    rscDfn,
                    LinstorParsingUtils.asVlmNr(vlmDfnMetaEntry.getKey()),
                    null,
                    vlmDfnMetaEntry.getValue().getSize(),
                    VolumeDefinition.Flags.restoreFlags(vlmDfnMetaEntry.getValue().getFlags())
                );
            }
            else if (resetData)
            {
                vlmDfn.getFlags().resetFlagsTo(
                    peerAccCtx.get(),
                    VolumeDefinition.Flags.restoreFlags(vlmDfnMetaEntry.getValue().getFlags())
                );
                try
                {
                    vlmDfn.setVolumeSize(peerAccCtx.get(), vlmDfnMetaEntry.getValue().getSize());
                }
                catch (MinSizeException | MaxSizeException exc)
                {
                    throw new ImplementationError("Invalid size during backup restore", exc);
                }
            }
            Map<String, String> vlmDfnMetaProps = vlmDfnMetaEntry.getValue().getVlmDfnProps();
            PropsUtils.resetProps(vlmDfnMetaProps, vlmDfn.getProps(peerAccCtx.get()));
            totalSize += vlmDfnMetaEntry.getValue().getSize();

            SnapshotVolumeDefinition snapVlmDfn = snapshotCrtHelper.createSnapshotVlmDfnData(snapDfn, vlmDfn);
            PropsUtils.resetProps(vlmDfnMetaProps, snapVlmDfn.getVlmDfnPropsForChange(peerAccCtx.get()));
            PropsUtils.resetProps(
                vlmDfnMetaEntry.getValue().getSnapVlmDfnProps(),
                snapVlmDfn.getSnapVlmDfnProps(peerAccCtx.get())
            );
            snapVlmDfns.put(vlmDfnMetaEntry.getKey(), snapVlmDfn);
        }
        return totalSize;
    }

    /**
     * Creates the snapDfn based on the given metadata and sets all props and flags needed for the receive and restore
     */
    private SnapshotDefinition getSnapDfnForBackupRestore(
        BackupMetaDataPojo metadata,
        SnapshotName snapName,
        ResourceDefinition rscDfn,
        ApiCallRcImpl responsesRef,
        AbsRemote remote,
        boolean downloadOnly,
        boolean forceRestore
    )
        throws AccessDeniedException, DatabaseException
    {
        backupHelper.ensureShippingToRemoteAllowed(remote);

        AccessContext accCtx = peerAccCtx.get();
        // if the snapDfn exists here, it can only be empty. While this should not be possible (since the delete of the
        // last snap should also delete the snapDfn), we don't want to run into an exception in case this somehow
        // happens anyways. Therefore we just treat the empty snapDfn as if it had just been created.
        @Nullable SnapshotDefinition snapDfn = rscDfn.getSnapshotDfn(accCtx, snapName);
        if (snapDfn == null)
        {
            snapDfn = snapshotCrtHelper.createSnapshotDfnData(
                rscDfn,
                snapName,
                new SnapshotDefinition.Flags[]
                {}
            );
        }
        PropsUtils.resetProps(metadata.getRscDfn().getSnapDfnProps(), snapDfn.getSnapDfnProps(accCtx));
        Props snapRscDfnProps = snapDfn.getRscDfnPropsForChange(accCtx);
        PropsUtils.resetProps(metadata.getRscDfn().getRscDfnProps(), snapRscDfnProps);
        // force the node to become primary afterwards in case we needed to recreate
        // the metadata
        snapRscDfnProps.removeProp(InternalApiConsts.PROP_PRIMARY_SET);

        snapDfn.getFlags().enableFlags(
            accCtx,
            SnapshotDefinition.Flags.SHIPPING,
            SnapshotDefinition.Flags.BACKUP
        );

        int rscCt = rscDfn.getResourceCount();
        if (rscCt != 0)
        {
            if (rscCt == 1 && forceRestore)
            {
                snapDfn.getFlags()
                    .enableFlags(accCtx, SnapshotDefinition.Flags.FORCE_RESTORE_BACKUP_ON_SUCCESS);
            }
            else if (!downloadOnly)
            {
                responsesRef.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.WARN_BACKUP_DL_ONLY,
                        "The target resource-definition is already deployed on nodes. " +
                            "After downloading the Backup Linstor will NOT restore the data to prevent unintentional " +
                            "data-loss."
                    )
                );
            }
        }
        else if (!downloadOnly) // ignore forceRestore, nothing to force
        {
            snapDfn.getFlags().enableFlags(accCtx, SnapshotDefinition.Flags.RESTORE_BACKUP_ON_SUCCESS);
        }

        return snapDfn;
    }

    /**
     * Loads or creates the rscDfn and sets all props based on the given metadata
     */
    private ResourceDefinition getRscDfnForBackupRestore(
        String targetRscName,
        SnapshotName snapName,
        BackupMetaDataPojo metadata,
        boolean resetData,
        @Nullable String dstRscGrpRef,
        boolean forceMoveRscGrpRef,
        ApiCallRcImpl responsesRef
    )
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(targetRscName, false);
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();

        try
        {
            if (rscDfn == null)
            {
                rscDfn = ctrlRscDfnApiCallHandler.createResourceDefinition(
                    targetRscName,
                    null,
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    null,
                    dstRscGrpRef,
                    true,
                    apiCallRcs,
                    false
                );
            }
            else if (backupInfoMgr.restoreContainsRscDfn(rscDfn))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_IN_USE | ApiConsts.MASK_BACKUP,
                        "A backup is currently being restored to resource definition " + targetRscName + "."
                    )
                );
            }
            if (resetData)
            {
                rscDfn.getFlags()
                    .resetFlagsTo(
                        peerAccCtx.get(),
                        ResourceDefinition.Flags.restoreFlags(metadata.getRscDfn().getFlags())
                    );
                Props rscDfnProps = rscDfn.getProps(peerAccCtx.get());
                PropsUtils.resetProps(metadata.getRscDfn().getRscDfnProps(), rscDfnProps);

                // force the node to become primary afterwards in case we needed to recreate
                // the metadata
                rscDfnProps.removeProp(InternalApiConsts.PROP_PRIMARY_SET);

                // if we already reset data, we can also move the resource-group, regardless if --force-rsc-grp was set
                // or not
                if (dstRscGrpRef != null && !dstRscGrpRef.isBlank() &&
                    !rscDfn.getResourceGroup().getName().value.equalsIgnoreCase(dstRscGrpRef))
                {
                    rscDfn.setResourceGroup(sysCtx, ctrlApiDataLoader.loadResourceGroup(dstRscGrpRef, true));
                }
            }
            else
            {
                if (dstRscGrpRef != null && !dstRscGrpRef.isBlank() &&
                    !rscDfn.getResourceGroup().getName().value.equalsIgnoreCase(dstRscGrpRef))
                {
                    if (rscDfn.getResourceCount() == 0 || forceMoveRscGrpRef)
                    {
                        rscDfn.setResourceGroup(sysCtx, ctrlApiDataLoader.loadResourceGroup(dstRscGrpRef, true));
                    }
                    else
                    {
                        responsesRef.add(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.WARN_RSC_ALREADY_DEPLOYED,
                                String.format(
                                    "Target resource definition '%s' has resources deployed. --target-resource-group " +
                                        "is ignored in order to prevent unexpected future autoplacements. " +
                                        "Use --force-move-resource-group to override this warning.",
                                    rscDfn.getName().displayValue
                                )
                            )
                        );
                    }
                }
            }
            rscDfn.getFlags().enableFlags(peerAccCtx.get(), ResourceDefinition.Flags.RESTORE_TARGET);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "restore backup",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN | ApiConsts.MASK_BACKUP
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        return rscDfn;
    }

    /**
     * Do the same as restoreBackup, but with a few changes to account for l2l-shipping.
     * The main differences are how to find the base snapshot and the order in which things are done
     */
    @SuppressWarnings("unchecked")
    Flux<BackupShippingStartInfo> restoreBackupL2LInTransaction(
        BackupShippingDstData data
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        Flux<BackupShippingStartInfo> ret;

        try
        {
            // 5. create layerPayload
            RscLayerDataApi layers = data.getMetaData().getLayerData();
            // 8. create rscDfn
            @Nullable String dstRscGrp = data.getDstRscGrp();
            ResourceDefinition rscDfn = getRscDfnForBackupRestore(
                data.getDstRscName(),
                data.getSnapName(),
                data.getMetaData(),
                data.isResetData(),
                dstRscGrp,
                data.isForceRscGrp(),
                responses
            );

            SnapshotDefinition snapDfn;
            Map<Integer, SnapshotVolumeDefinition> snapVlmDfns = new TreeMap<>();
            Set<StorPool> storPools;
            Snapshot snap = null;

            snapDfn = getSnapDfnForBackupRestore(
                data.getMetaData(),
                data.getSnapName(),
                rscDfn,
                responses,
                backupHelper.getRemote(data.getStltRemote().getLinstorRemoteName().displayValue),
                data.isDownloadOnly(),
                data.isForceRestore()
            );
            // 10. create vlmDfn(s)
            // 11. create snapVlmDfn(s)
            long totalSize = createSnapVlmDfnForBackupRestore(
                data.getDstRscName(),
                data.getMetaData(),
                rscDfn,
                snapDfn,
                snapVlmDfns,
                data.isResetData()
            );
            Snapshot incrementalBaseSnap = null;
            if (data.getDstBaseSnapName() != null)
            {
                SnapshotDefinition baseSnapDfn = ctrlApiDataLoader.loadSnapshotDfn(
                    data.getDstRscName(),
                    data.getDstBaseSnapName(),
                    false
                );
                if (baseSnapDfn != null)
                {
                    Node baseNode = ctrlApiDataLoader.loadNode(data.getDstActualNodeName(), true);
                    incrementalBaseSnap = baseSnapDfn.getSnapshot(sysCtx, baseNode.getName());
                }
            }
            if (incrementalBaseSnap != null)
            {
                data.getMetaData()
                    .getRsc()
                    .getSnapProps()
                    .put(
                    ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" + InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                    incrementalBaseSnap.getSnapshotName().displayValue
                );
                storPools = LayerVlmUtils.getStorPools(incrementalBaseSnap, sysCtx, false);
                if (storPools.size() != 1)
                {
                    throw new ImplementationError("Received snapshot unexpectedly has more than one storage pool");
                }
                StorPool storPool = storPools.iterator().next();

                // test if given storage pool is usable
                long freeSpace = storPool.getFreeSpaceTracker().getFreeCapacityLastUpdated(sysCtx).orElse(0L);
                long allocatedSizeSum = getAllocatedSizeSum(incrementalBaseSnap);
                if (freeSpace < totalSize - allocatedSizeSum)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_NOT_ENOUGH_FREE_SPACE,
                            "Storage pool does not have enough free space left"
                        )
                    );
                }
                if (!storPool.getDeviceProviderKind().equals(getProviderKind(layers)))
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_STOR_DRIVER,
                            "Storage pool with base snapshot is of type " + storPool.getDeviceProviderKind() +
                                " but incremental snapshot would be of type: " + getProviderKind(layers)
                        )
                    );
                }
            }
            else
            {
                Map<ExtTools, ExtToolsInfo.Version> extTools = new HashMap<>();
                extTools.put(ExtTools.SOCAT, null);
                if (data.isUseZstd())
                {
                    extTools.put(ExtTools.ZSTD, null);
                }
                AutoSelectFilterBuilder autoSelectBuilder = new AutoSelectFilterBuilder()
                    .setPlaceCount(1)
                    .setNodeNameList(data.getDstNodeName() == null ? null : Arrays.asList(data.getDstNodeName()))
                    .setStorPoolNameList(data.getDstStorPool() == null ? null : Arrays.asList(data.getDstStorPool()))
                    .setLayerStackList(getLayerList(layers))
                    .setDeviceProviderKinds(Arrays.asList(getProviderKind(layers)))
                    .setDisklessOnRemaining(false)
                    .setSkipAlreadyPlacedOnAllNodeCheck(true)
                    .setRequireExtTools(extTools);
                storPools = autoplacer.autoPlace(
                    autoSelectBuilder.build(),
                    rscDfn,
                    totalSize
                );
                if ((storPools == null || storPools.isEmpty()) && data.isUseZstd())
                {
                    // try not using it..
                    data.setUseZstd(false);
                    extTools.remove(ExtTools.ZSTD);
                    storPools = autoplacer.autoPlace(
                        autoSelectBuilder.build(),
                        rscDfn,
                        totalSize
                    );
                }
            }
            if (storPools == null || storPools.isEmpty())
            {
                ret = Flux.error(
                    new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_VLM_SIZE,
                            "Could not find suitable storage pool to receive backup"
                        )
                    )
                );
            }
            else
            {
                StorPool storPool = storPools.iterator().next();
                Node node = storPool.getNode();

                // TODO add autoSelected storPool into renameMap
                // 12. create snapshot
                snap = createSnapshotAndVolumesForBackupRestore(
                    data.getMetaData(),
                    layers,
                    node,
                    snapDfn,
                    snapVlmDfns,
                    data.getStorPoolRenameMap(),
                    data.getStltRemote(),
                    responses
                );
                snap.getSnapProps(peerAccCtx.get())
                    .setProp(
                        InternalApiConsts.KEY_BACKUP_TO_RESTORE,
                        snap.getResourceName().displayValue + "_" + snap.getSnapshotName().displayValue,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );
                snap.getFlags().enableFlags(peerAccCtx.get(), Snapshot.Flags.BACKUP_TARGET);
                if (!backupInfoMgr.addAllRestoreEntries(
                    rscDfn,
                    "",
                    data.getDstRscName(),
                    Collections.singletonList(snap),
                    Collections.emptyMap(),
                    data.getStltRemote().getLinstorRemoteName()
                    )
                )
                {
                    throw new ImplementationError(
                        "Tried to overwrite existing backup-info-mgr entry for rscDfn " + data.getDstRscName()
                    );
                }
                // add to mgr so that when port is decided src-cluster can be contacted
                backupInfoMgr.addL2LDstData(snap, data);
                // LUKS
                Set<AbsRscLayerObject<Snapshot>> luksLayerData = LayerRscUtils.getRscDataByLayer(
                    snap.getLayerData(sysCtx),
                    DeviceLayerKind.LUKS
                );
                if (!luksLayerData.isEmpty())
                {
                    LuksLayerMetaPojo luksInfo = data.getMetaData().getLuksInfo();
                    if (luksInfo == null)
                    {
                        throw new ImplementationError("Cannot receive LUKS data without LuksInfo");
                    }
                    byte[] remoteMasterKey = getRemoteMasterKey(data.getSrcClusterId(), luksInfo);
                    if (remoteMasterKey == null)
                    {
                        throw new ImplementationError(
                            "Source cluster master key could not be restored. Is the passphrase correct?"
                        );
                    }
                    try
                    {
                        for (AbsRscLayerObject<Snapshot> luksRscData : luksLayerData)
                        {
                            for (VlmProviderObject<Snapshot> vlm : luksRscData.getVlmLayerObjects().values())
                            {
                                LuksVlmData<Snapshot> luksVlm = (LuksVlmData<Snapshot>) vlm;
                                byte[] vlmKey = luksVlm.getEncryptedKey();
                                byte[] decryptedKey = decHelper.decrypt(remoteMasterKey, vlmKey);

                                byte[] encVlmKey = encHelper.encrypt(decryptedKey);
                                luksVlm.setEncryptedKey(encVlmKey);
                            }
                        }
                    }
                    catch (LinStorException exc)
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                                "De- or encrypting the volume passwords failed."
                            ),
                            exc
                        );
                    }
                }

                data.getStltRemote().useZstd(sysCtx, data.isUseZstd());

                // update stlts
                ctrlTransactionHelper.commit();

                // calling ...SupressingErrorClasses without classes => do not ignore DelayedApiRcException. we want to
                // deal with that by our self
                ret = ctrlSatelliteUpdateCaller.updateSatellites(data.getStltRemote())
                    .thenMany(
                        snapshotCrtHandler.postCreateSnapshotSuppressingErrorClasses(snapDfn, true)
                            .onErrorResume(
                                error -> cleanupAfterFailedRestore(
                                    error,
                                    snapDfn,
                                    data.getDstRscName()
                                )
                            )
                            .map(apiCallRcList ->
                                {
                                    responses.addEntries(apiCallRcList);
                                    return (ApiCallRc) responses;
                                }
                            )
                            .thenMany(
                                Flux.just(
                                    new BackupShippingStartInfo(
                                        new ApiCallRcWith<>(responses, snap),
                                        incrementalBaseSnap
                                    )
                                )
                            )
                    );
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "restore backup",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN | ApiConsts.MASK_BACKUP
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        catch (InvalidKeyException | InvalidValueException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    /**
     * Finds the correct base snapshot for l2l-shipping
     */
    Snapshot getIncrementalBaseL2LPrivileged(
        ResourceDefinition rscDfnRef,
        Set<String> srcSnapDfnUuidsForIncrementalRef,
        @Nullable String dstNodeRef,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException
    {
        Snapshot ret = null;
        NodeName dstNode = null;
        if (dstNodeRef != null)
        {
            try
            {
                Node node = ctrlApiDataLoader.loadNode(new NodeName(dstNodeRef), false);
                if (node != null)
                {
                    dstNode = node.getName();
                }
                else
                {
                    apiCallRc.addEntries(
                        ApiCallRcImpl.singleApiCallRc(
                            ApiConsts.WARN_NOT_FOUND,
                            "Preferred target node does not exist, choosing different node instead"
                        )
                    );
                }
            }
            catch (InvalidNameException exc)
            {
                apiCallRc.addEntries(
                    ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.WARN_NOT_FOUND,
                        "Preferred target node name is not valid, choosing different node instead"
                    )
                );
            }
        }

        long latestTimestamp = -1;
        for (SnapshotDefinition snapDfn : rscDfnRef.getSnapshotDfns(sysCtx))
        {
            Props snapDfnProps = snapDfn.getSnapDfnProps(sysCtx);
            String fromSrcSnapDfnUuid = snapDfnProps
                .getProp(InternalApiConsts.KEY_BACKUP_L2L_SRC_SNAP_DFN_UUID);
            if (srcSnapDfnUuidsForIncrementalRef.contains(fromSrcSnapDfnUuid))
            {
                Snapshot snap;
                /*
                 * If the user chose a specific node, try to place the snap there even if it means we can't make an inc
                 * or the inc has to be based on a snap a lot further back
                 */
                if (dstNode != null)
                {
                    snap = snapDfn.getSnapshot(sysCtx, dstNode);
                }
                else
                {
                    Iterator<Snapshot> snapshotIterator = snapDfn.getAllSnapshots(sysCtx).iterator();
                    if (!snapshotIterator.hasNext())
                    {
                        throw new ImplementationError(
                            "snapdfn: " + CtrlSnapshotApiCallHandler.getSnapshotDfnDescriptionInline(snapDfn) +
                                " has no snapshots!"
                        );
                    }
                    /*
                     * snapshotDfn will have only one snapshot (we will certainly not have received a backup into
                     * multiple nodes)
                     * WARNING this might not stay this way
                     */
                    snap = snapshotIterator.next();
                }
                /*
                 * TODO: This should also test whether we have enough space left over to receive the snap, and since we
                 * don't really have a good way to find out how big an incremental snap would be, we need to check for
                 * enough space for a full backup.
                 * If there isn't enough space, we need to make a full backup to a different storpool/node, or
                 * completely abort the shipping
                 */
                long timestamp = Long.parseLong(
                    snapDfnProps.getProp(
                        InternalApiConsts.KEY_BACKUP_START_TIMESTAMP,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    )
                );
                /*
                 * only update if snap is not null, we only want to return null if there is absolutely no way to put an
                 * incremental snap on the node the user wants
                 */
                if (timestamp > latestTimestamp && snap != null)
                {
                    latestTimestamp = timestamp;
                    ret = snap;
                }
            }
        }
        return ret;
    }

    /**
     * Finds out how much space the given snapshot uses
     */
    private long getAllocatedSizeSum(Snapshot snapRef)
    {
        long allocatedSizeSum = 0;
        try
        {
            Set<AbsRscLayerObject<Snapshot>> rscDataByProvider = LayerRscUtils
                .getRscDataByLayer(snapRef.getLayerData(sysCtx), DeviceLayerKind.STORAGE);
            for (AbsRscLayerObject<Snapshot> storRscData : rscDataByProvider)
            {
                for (VlmProviderObject<Snapshot> storVlmData : storRscData.getVlmLayerObjects().values())
                {
                    allocatedSizeSum += storVlmData.getAllocatedSize();
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return allocatedSizeSum;
    }

    /**
     * Returns a list of all layers that are part of the data path
     */
    private List<DeviceLayerKind> getLayerList(RscLayerDataApi layersRef)
    {
        List<DeviceLayerKind> ret = new ArrayList<>();
        RscLayerDataApi current = layersRef;
        do
        {
            ret.add(current.getLayerKind());
            List<RscLayerDataApi> children = current.getChildren();
            current = null;
            for (RscLayerDataApi child : children)
            {
                if (child.getRscNameSuffix().equals(RscLayerSuffixes.SUFFIX_DATA))
                {
                    current = child;
                }
            }
        }
        while (current != null);

        return ret;
    }

    /**
     * Finds the storage-layer of the data-path and then makes sure all vlms have the same provider kind
     */
    private DeviceProviderKind getProviderKind(RscLayerDataApi layersRef)
    {
        DeviceProviderKind providerKind = null;

        RscLayerDataApi current = layersRef;
        while (current != null && !current.getLayerKind().equals(DeviceLayerKind.STORAGE))
        {
            List<RscLayerDataApi> children = current.getChildren();
            current = null;
            for (RscLayerDataApi child : children)
            {
                if (child.getRscNameSuffix().equals(RscLayerSuffixes.SUFFIX_DATA))
                {
                    current = child;
                }
            }
        }
        if (current == null)
        {
            throw new ImplementationError("No STORAGE layer found.");
        }

        for (VlmLayerDataApi vlmLayerDataApi : current.getVolumeList())
        {
            DeviceProviderKind kind = vlmLayerDataApi.getProviderKind();
            if (providerKind == null)
            {
                providerKind = kind;
            }
            else
            {
                if (!providerKind.equals(kind))
                {
                    throw new ImplementationError(
                        "Backup shipping with volumes of different provider kinds is not (yet) supported!"
                    );
                }
            }
        }

        return providerKind;
    }

    /**
     * Returns the master key from the source cluster
     */
    private byte[] getRemoteMasterKey(String sourceClusterIdStr, LuksLayerMetaPojo luksInfo)
        throws AccessDeniedException
    {
        byte[] remoteMasterKey = null;
        UUID srcClusterId = UUID.fromString(sourceClusterIdStr);
        for (AbsRemote sourceRemote : remoteRepo.getMapForView(sysCtx).values())
        {
            if (sourceRemote instanceof LinstorRemote)
            {
                LinstorRemote linstorSrcRemote = (LinstorRemote) sourceRemote;
                UUID remoteClusterId = linstorSrcRemote.getClusterId(sysCtx);
                if (Objects.equals(remoteClusterId, srcClusterId))
                {
                    byte[] encryptedRemotePassphrase = linstorSrcRemote.getEncryptedRemotePassphrase(sysCtx);
                    if (encryptedRemotePassphrase == null)
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY | ApiConsts.MASK_BACKUP,
                                "The resource to be shipped seems to have luks configured, but no passphrase was given."
                            )
                        );
                    }
                    try
                    {
                        byte[] remotePassphrase = decHelper
                            .decrypt(backupHelper.getLocalMasterKey(), encryptedRemotePassphrase);
                        remoteMasterKey = encHelper.getDecryptedMasterKey(
                            luksInfo.getMasterCryptHash(),
                            luksInfo.getMasterPassword(),
                            luksInfo.getMasterCryptSalt(),
                            new String(remotePassphrase, StandardCharsets.UTF_8)
                        );
                    }
                    catch (LinStorException exc)
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                                "De- or encrypting the volume passwords failed."
                            ),
                            exc
                        );
                    }
                    break;
                }
            }
        }

        return remoteMasterKey;
    }

    /**
     * Called by the stlt as soon as it finishes receiving the backup
     */
    public Flux<ApiCallRc> shippingReceived(
        String rscNameRef,
        String snapNameRef,
        List<Integer> portsRef,
        boolean successRef
    )
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Finish receiving backup",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> shippingReceivedInTransaction(rscNameRef, snapNameRef, portsRef, successRef)
            );
    }

    /**
     * Makes sure all props and flags that trigger a receive are removed properly, then continues
     * based on successRef.
     * If the receiving was unsuccessful, cleans up all snaps created for this receive.
     * Otherwise, either starts the receive for the next incremental backup, or starts restoring.
     */
    private Flux<ApiCallRc> shippingReceivedInTransaction(
        String rscNameRef,
        String snapNameRef,
        List<Integer> portsRef,
        boolean successRef
    )
    {
        errorReporter.logInfo(
            "Backup receiving for snapshot %s of resource %s %s", snapNameRef, rscNameRef,
            successRef ? "finished successfully" : "failed"
        );
        Flux<ApiCallRc> flux = Flux.empty();
        SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameRef, snapNameRef, false);
        if (snapDfn != null)
        {
            try
            {
                AccessContext peerCtx = peerAccCtx.get();

                NodeName nodeName = peerProvider.get().getNode().getName();
                Snapshot snap = snapDfn.getSnapshot(peerCtx, nodeName);
                boolean deletingSnap = false;

                StateFlags<Flags> snapDfnFlags = snapDfn.getFlags();
                snapDfnFlags.disableFlags(peerCtx, SnapshotDefinition.Flags.SHIPPING);
                if (snap != null && !snap.isDeleted())
                {
                    Props snapProps = snap.getSnapProps(peerCtx);
                    snapProps.removeProp(
                        InternalApiConsts.KEY_BACKUP_TO_RESTORE,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );

                    for (Integer port : portsRef)
                    {
                        if (port != null)
                        {
                            snapshotShippingPortPool.deallocate(port);
                        }
                    }

                    boolean keepGoing;
                    String remoteName = snapProps.removeProp(
                        InternalApiConsts.KEY_BACKUP_SRC_REMOTE,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );
                    AbsRemote remote = remoteRepo.get(sysCtx, new RemoteName(remoteName, true));
                    Snapshot nextSnap = backupInfoMgr.getNextBackupToDownload(snap);
                    if (successRef && nextSnap != null)
                    {
                        snapDfnFlags.enableFlags(peerCtx, SnapshotDefinition.Flags.SHIPPED);
                        flux = ctrlRemoteApiCallHandler.cleanupRemotesIfNeeded(
                            backupInfoMgr.abortRestoreDeleteEntry(rscNameRef, snap)
                        );
                        SnapshotDefinition nextSnapDfn = nextSnap.getSnapshotDefinition();
                        nextSnapDfn.getFlags().enableFlags(peerCtx, SnapshotDefinition.Flags.SHIPPING);
                        nextSnap.getFlags().enableFlags(peerCtx, Snapshot.Flags.BACKUP_TARGET);

                        ctrlTransactionHelper.commit();
                        flux = flux.concatWith(
                            ctrlSatelliteUpdateCaller.updateSatellites(
                                snapDfn,
                                CtrlSatelliteUpdateCaller.notConnectedWarn()
                            ).transform(
                                responses -> CtrlResponseUtils.combineResponses(
                                    errorReporter,
                                    responses,
                                    LinstorParsingUtils.asRscName(rscNameRef),
                                    "Finishing receiving of backup ''" + snapNameRef + "'' of {1} on {0}"
                                )
                            ).concatWith(snapshotCrtHandler.postCreateSnapshot(nextSnapDfn, true))
                        );
                        keepGoing = true;
                    }
                    else
                    {
                        if (successRef)
                        {
                            snapDfnFlags.enableFlags(peerCtx, SnapshotDefinition.Flags.SHIPPED);
                            flux = ctrlRemoteApiCallHandler.cleanupRemotesIfNeeded(
                                backupInfoMgr.removeAllRestoreEntries(
                                    snapDfn.getResourceDefinition(),
                                    rscNameRef,
                                    snap
                                )
                            );
                            // start snap-restore
                            int rscCt = snapDfn.getResourceDefinition().getResourceCount();
                            if (snapDfnFlags.isSet(peerCtx, SnapshotDefinition.Flags.FORCE_RESTORE_BACKUP_ON_SUCCESS) &&
                                rscCt > 0)
                            {
                                PriorityProps prioProps = new PriorityProps(
                                    snapDfn.getResourceDefinition().getProps(peerCtx),
                                    snapDfn.getResourceDefinition().getResourceGroup().getProps(peerCtx),
                                    systemConfRepository.getCtrlConfForView(peerCtx)
                                );
                                boolean forceRestoreAllowed = ApiConsts.VAL_TRUE.equalsIgnoreCase(
                                    prioProps.getProp(
                                        ApiConsts.KEY_ALLOW_FORCE_RESTORE,
                                        ApiConsts.NAMESPC_BACKUP_SHIPPING,
                                        ApiConsts.VAL_TRUE
                                    )
                                );
                                boolean inUse = snapDfn.getResourceDefinition().anyResourceInUse(peerCtx).isPresent();
                                if (forceRestoreAllowed && !inUse && rscCt == 1)
                                {
                                    flux = ctrlRscDelApiCallHandler.deleteResource(nodeName.displayValue, rscNameRef);
                                }
                                else
                                {
                                    List<String> problemDetails = new ArrayList<>();
                                    if (!forceRestoreAllowed)
                                    {
                                        problemDetails.add(
                                            String.format(
                                                " * The property %s/%s is not effectively set to False on the target " +
                                                    "resource-definition (property is inherited from resource-group " +
                                                    "and controller)",
                                                ApiConsts.NAMESPC_BACKUP_SHIPPING,
                                                ApiConsts.KEY_ALLOW_FORCE_RESTORE
                                            )
                                        );
                                    }
                                    if (inUse)
                                    {
                                        problemDetails.add(" * The target resource-definition is not in use");
                                    }
                                    if (rscCt > 1)
                                    {
                                        problemDetails.add(" * The target resource-definition has at most 1 resource");
                                    }
                                    if (!problemDetails.isEmpty())
                                    {
                                        errorReporter.reportProblem(
                                            Level.WARN,
                                            new LinStorException(
                                                "Force restore option was used, but conditions are not met",
                                                null,
                                                null,
                                                null,
                                                "Make sure that: \n" + StringUtils.join(problemDetails, "\n")
                                            ),
                                            peerCtx,
                                            null,
                                            snapNameRef
                                        );
                                        // disables both, FORCE_RESTORE and RESTORE
                                        snapDfnFlags.disableFlags(
                                            peerCtx,
                                            SnapshotDefinition.Flags.FORCE_RESTORE_BACKUP_ON_SUCCESS
                                        );
                                    }
                                }
                            }
                            if (snapDfnFlags.isSet(peerCtx, SnapshotDefinition.Flags.RESTORE_BACKUP_ON_SUCCESS))
                            {
                                // make sure to not restore it a second time; FORCE_RESTORE... disables both flags
                                snapDfnFlags.disableFlags(
                                    peerCtx,
                                    SnapshotDefinition.Flags.FORCE_RESTORE_BACKUP_ON_SUCCESS
                                );

                                flux = flux.concatWith(
                                    ctrlSnapRestoreApiCallHandler.restoreSnapshotFromBackup(
                                        Collections.emptyList(),
                                        snapNameRef,
                                        rscNameRef
                                    )
                                );
                            }
                            else
                            {
                                /*
                                 * no need for a "successfully downloaded" message, as this flux is triggered
                                 * by the satellite who does not care about this kind of ApiCallRc message
                                 */
                            }
                            keepGoing = false; // we received the last backup
                        }
                        else
                        {
                            List<SnapshotDefinition> snapsToDelete = new ArrayList<>();
                            snapsToDelete.add(snapDfn);
                            backupInfoMgr.abortRestoreDeleteEntry(rscNameRef, snap);
                            Snapshot nextSnapToDel = nextSnap;
                            while (nextSnapToDel != null)
                            {
                                snapsToDelete.add(nextSnapToDel.getSnapshotDefinition());
                                nextSnapToDel.getSnapshotDefinition().getFlags()
                                    .enableFlags(peerCtx, SnapshotDefinition.Flags.SHIPPING_ABORT);
                                backupInfoMgr.abortRestoreDeleteEntry(rscNameRef, nextSnapToDel);
                                nextSnapToDel = backupInfoMgr.getNextBackupToDownload(nextSnapToDel);
                            }
                            Set<Snapshot> leftovers = backupInfoMgr.abortRestoreGetEntries(rscNameRef);
                            if (leftovers != null && leftovers.isEmpty())
                            {
                                ctrlRemoteApiCallHandler.cleanupRemotesIfNeeded(
                                    backupInfoMgr.removeAllRestoreEntries(
                                        snapDfn.getResourceDefinition(),
                                        rscNameRef,
                                        snap
                                    )
                                );
                            }
                            else
                            {
                                throw new ImplementationError(
                                    "Not all restore-entries marked for abortion: " + leftovers
                                );
                            }
                            flux = Flux.empty();
                            for (SnapshotDefinition snapDfnToDel : snapsToDelete)
                            {
                                flux = flux.concatWith(
                                    ctrlSnapDeleteApiCallHandler.deleteSnapshot(
                                        snapDfnToDel.getResourceName().displayValue,
                                        snapDfnToDel.getName().displayValue,
                                        null
                                    )
                                );
                            }
                            // no need to remove the BACKUP_TARGET flag if we are deleting the snap anyways in the next
                            // flux step
                            deletingSnap = true;
                            keepGoing = false; // last backup failed.
                        }
                    }

                    if (!keepGoing)
                    {
                        // commit any unsaved props-changes and update the stlts
                        ctrlTransactionHelper.commit();

                        Flux<ApiCallRc> delPropFlux = ctrlSatelliteUpdateCaller.updateSatellites(
                            snapDfn,
                            CtrlSatelliteUpdateCaller.notConnectedWarn()
                        ).transform(
                            responses -> CtrlResponseUtils.combineResponses(
                                errorReporter,
                                responses,
                                LinstorParsingUtils.asRscName(rscNameRef),
                                "Removing remote property from snapshot '" + snapNameRef + "' of {1} on {0}"
                            )
                        );

                        if (remote instanceof StltRemote)
                        {
                            // cleanupStltRemote will not be executed if flux has an error - this issue is currently
                            // unavoidable.
                            // This will be fixed with the linstor2 issue 19 (Combine Changed* proto messages for atomic
                            // updates)
                            delPropFlux = delPropFlux.concatWith(backupHelper.cleanupStltRemote((StltRemote) remote));
                            // since we have a stltRemote, we are at the end of an l2l shipping. This means that we need
                            // to tell the src-cluster that we are done with the download.
                            BackupShippingDstData data = backupInfoMgr.getL2LDstData(snap);
                            backupInfoMgr.removeL2LDstData(snap);
                            BackupShippingReceiveDoneRequest request = new BackupShippingReceiveDoneRequest(
                                new ApiCallRcImpl(),
                                data.getSrcL2LRemoteName(),
                                data.getSrcStltRemoteName(),
                                data.getSrcL2LRemoteUrl()
                            );
                            flux = flux.concatWith(
                                backupShippingRestClient.sendBackupReceiveDoneRequest(request)
                                    .map(Json::jsonToApiCallRc)
                            );
                        }
                        /*
                         * We need to update the stlts with the flag- & prop-changes before starting the restore
                         */
                        flux = delPropFlux.concatWith(flux);
                    }
                }
                ctrlTransactionHelper.commit();
                if (snap != null && !deletingSnap)
                {
                    flux = flux.concatWith(cleanupBackupTarget(snap));
                }
                flux = flux.concatWith(
                    backupHelper.startStltCleanup(
                        peerProvider.get(),
                        rscNameRef,
                        snapNameRef
                    )
                );
            }
            catch (AccessDeniedException | InvalidKeyException | InvalidNameException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (DatabaseException exc)
            {
                throw new ApiDatabaseException(exc);
            }
        }
        return flux;
    }

    private Flux<ApiCallRc> cleanupBackupTarget(Snapshot snapRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Removing BACKUP_TARGET flag",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> cleanupBackupTargetInTransaction(snapRef)
            );
    }

    private Flux<ApiCallRc> cleanupBackupTargetInTransaction(Snapshot snapRef)
    {
        try
        {
            snapRef.getFlags().disableFlags(peerAccCtx.get(), Snapshot.Flags.BACKUP_TARGET);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        ctrlTransactionHelper.commit();
        return ctrlSatelliteUpdateCaller.updateSatellites(
            snapRef.getSnapshotDefinition(),
            CtrlSatelliteUpdateCaller.notConnectedWarn())
                .transform(
                responses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    responses,
                    snapRef.getResourceName(),
                    "Removing BACKUP_TARGET flag from snapshot '" + snapRef + "' of {1} on {0}"
                )
            );
    }
}
