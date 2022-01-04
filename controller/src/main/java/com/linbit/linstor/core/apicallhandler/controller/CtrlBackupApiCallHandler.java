package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.BackupToS3;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.backups.BackupInfoPojo;
import com.linbit.linstor.api.pojo.backups.BackupInfoStorPoolPojo;
import com.linbit.linstor.api.pojo.backups.BackupInfoVlmPojo;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.api.pojo.backups.BackupMetaInfoPojo;
import com.linbit.linstor.api.pojo.backups.BackupPojo;
import com.linbit.linstor.api.pojo.backups.BackupPojo.BackupS3Pojo;
import com.linbit.linstor.api.pojo.backups.BackupPojo.BackupVlmS3Pojo;
import com.linbit.linstor.api.pojo.backups.BackupPojo.BackupVolumePojo;
import com.linbit.linstor.api.pojo.backups.LuksLayerMetaPojo;
import com.linbit.linstor.api.pojo.backups.VlmDfnMetaPojo;
import com.linbit.linstor.api.pojo.backups.VlmMetaPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.backupshipping.S3Consts;
import com.linbit.linstor.backupshipping.S3MetafileNameInfo;
import com.linbit.linstor.backupshipping.S3VolumeNameInfo;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlBackupL2LDstApiCallHandler.BackupShippingStartInfo;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.controller.exceptions.MissingKeyPropertyException;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apis.BackupApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.LinstorRemote;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Remote;
import com.linbit.linstor.core.objects.Remote.RemoteType;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.S3Remote;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StltRemote;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfProtectionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
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
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.backupshipping.S3Consts.META_SUFFIX;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

@Singleton
public class CtrlBackupApiCallHandler
{
    private final Provider<AccessContext> peerAccCtx;
    private final AccessContext sysCtx;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSnapshotCrtHelper snapshotCrtHelper;
    private final CtrlSnapshotCrtApiCallHandler snapshotCrtHandler;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ErrorReporter errorReporter;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final BackupToS3 backupHandler;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteApiCallHandler;
    private final CtrlRscDfnApiCallHandler ctrlRscDfnApiCallHandler;
    private final CtrlVlmDfnCrtApiHelper ctrlVlmDfnCrtApiHelper;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final CtrlSnapshotRestoreApiCallHandler ctrlSnapRestoreApiCallHandler;
    private final EncryptionHelper encHelper;
    private final DecryptionHelper decHelper;
    private final CtrlSecurityObjects ctrlSecObj;
    private final BackupInfoManager backupInfoMgr;
    private final Provider<Peer> peerProvider;
    private final CtrlSnapshotShippingAbortHandler ctrlSnapShipAbortHandler;
    private final RemoteRepository remoteRepo;
    private final SystemConfProtectionRepository sysCfgRepo;
    private final ResourceDefinitionRepository rscDfnRepo;
    private final Autoplacer autoplacer;
    private final CtrlStltSerializer stltComSerializer;
    private final DynamicNumberPool snapshotShippingPortPool;

    @Inject
    public CtrlBackupApiCallHandler(
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @SystemContext AccessContext sysCtxRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSnapshotCrtHelper snapCrtHelperRef,
        CtrlSnapshotCrtApiCallHandler snapshotCrtHandlerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ErrorReporter errorReporterRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        BackupToS3 backupHandlerRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteApiCallHandlerRef,
        CtrlRscDfnApiCallHandler ctrlRscDfnApiCallHandlerRef,
        CtrlVlmDfnCrtApiHelper ctrlVlmDfnCrtApiHelperRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        CtrlSnapshotRestoreApiCallHandler ctrlSnapRestoreApiCallHandlerRef,
        EncryptionHelper encHelperRef,
        DecryptionHelper decHelperRef,
        CtrlSecurityObjects ctrlSecObjRef,
        BackupInfoManager backupInfoMgrRef,
        Provider<Peer> peerProviderRef,
        CtrlSnapshotShippingAbortHandler ctrlSnapShipAbortHandlerRef,
        RemoteRepository remoteRepoRef,
        SystemConfProtectionRepository sysCfgRepoRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        Autoplacer autoplacerRef,
        CtrlStltSerializer ctrlComSerializerRef,
        @Named(NumberPoolModule.SNAPSHOPT_SHIPPING_PORT_POOL) DynamicNumberPool snapshotShippingPortPoolRef
    )
    {
        peerAccCtx = peerAccCtxRef;
        sysCtx = sysCtxRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        snapshotCrtHelper = snapCrtHelperRef;
        snapshotCrtHandler = snapshotCrtHandlerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        errorReporter = errorReporterRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        backupHandler = backupHandlerRef;
        ctrlSnapDeleteApiCallHandler = ctrlSnapDeleteApiCallHandlerRef;
        ctrlRscDfnApiCallHandler = ctrlRscDfnApiCallHandlerRef;
        ctrlVlmDfnCrtApiHelper = ctrlVlmDfnCrtApiHelperRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        ctrlSnapRestoreApiCallHandler = ctrlSnapRestoreApiCallHandlerRef;
        encHelper = encHelperRef;
        decHelper = decHelperRef;
        ctrlSecObj = ctrlSecObjRef;
        backupInfoMgr = backupInfoMgrRef;
        peerProvider = peerProviderRef;
        ctrlSnapShipAbortHandler = ctrlSnapShipAbortHandlerRef;
        remoteRepo = remoteRepoRef;
        sysCfgRepo = sysCfgRepoRef;
        rscDfnRepo = rscDfnRepoRef;
        autoplacer = autoplacerRef;
        stltComSerializer = ctrlComSerializerRef;
        snapshotShippingPortPool = snapshotShippingPortPoolRef;
    }

    public Flux<ApiCallRc> createBackup(
        String rscNameRef,
        String snapNameRef,
        String remoteNameRef,
        String nodeNameRef,
        boolean incremental
    )
        throws AccessDeniedException
    {
        return scopeRunner.fluxInTransactionalScope(
            "Backup snapshot",
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
                true,
                incremental,
                Collections.singletonMap(ExtTools.ZSTD, null),
                null,
                RemoteType.S3
            ).objA
        );
    }

    Pair<Flux<ApiCallRc>, Snapshot> backupSnapshot(
        String rscNameRef,
        String remoteName,
        String nodeName,
        String snapNameRef,
        Date nowRef,
        boolean setShippingFlag,
        boolean allowIncremental,
        Map<ExtTools, ExtToolsInfo.Version> requiredExtTools,
        Map<ExtTools, ExtToolsInfo.Version> optionalExtTools,
        RemoteType remoteTypeRef
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
            Collection<SnapshotDefinition> snapDfns = getInProgressBackups(rscDfn);
            if (!snapDfns.isEmpty())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_EXISTS_SNAPSHOT_SHIPPING,
                        "Backup shipping of resource '" + rscNameRef + "' already in progress"
                    )
                );
            }

            if (remoteTypeRef.equals(RemoteType.S3))
            {
                // check if encryption is possible
                getLocalMasterKey();

                // check if remote exists
                getS3Remote(remoteName);
            }

            SnapshotDefinition prevSnapDfn = getIncrementalBase(rscDfn, remoteName, allowIncremental);

            Pair<Node, List<String>> chooseNodeResult = chooseNode(
                rscDfn,
                prevSnapDfn,
                requiredExtTools,
                optionalExtTools
            );
            NodeName chosenNodeName = chooseNodeResult.objA.getName();
            List<String> nodes = chooseNodeResult.objB;

            boolean nodeNameFound = false;
            if (nodeName != null && !nodeName.isEmpty())
            {
                for (String possibleNodeName : nodes)
                {
                    if (nodeName.equalsIgnoreCase(possibleNodeName))
                    {
                        chosenNodeName = new NodeName(possibleNodeName);
                        nodeNameFound = true;
                        break;
                    }
                }
            }
            if (!nodeNameFound && nodeName != null)
            {
                responses.addEntry(
                    "Preferred node '" + nodeName + "' could not be selected. Choosing '" + chosenNodeName +
                        "' instead.",
                    ApiConsts.MASK_WARN
                );
            }

            SnapshotDefinition snapDfn = snapshotCrtHelper
                .createSnapshots(nodes, rscDfn.getName().displayValue, snapName, responses);
            snapDfn.getFlags()
                .enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING, SnapshotDefinition.Flags.BACKUP);

            snapDfn.getProps(peerAccCtx.get()).setProp(
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

            Resource rsc = rscDfn.getResource(peerAccCtx.get(), chosenNodeName);
            List<Integer> nodeIds = new ArrayList<>();
            Set<AbsRscLayerObject<Resource>> drbdLayers = LayerRscUtils
                .getRscDataByProvider(rsc.getLayerData(peerAccCtx.get()), DeviceLayerKind.DRBD);
            if (drbdLayers.size() > 1)
            {
                throw new ImplementationError("Only one instance of DRBD-layer supported");
            }
            for (AbsRscLayerObject<Resource> layer : drbdLayers)
            {
                DrbdRscData<Resource> drbdLayer = (DrbdRscData<Resource>) layer;
                boolean extMeta = false;
                boolean intMeta = false;
                for (DrbdVlmData<Resource> drbdVlm : drbdLayer.getVlmLayerObjects().values())
                {
                    if (drbdVlm.isUsingExternalMetaData())
                    {
                        extMeta = true;
                    }
                    else
                    {
                        intMeta = true;
                    }
                }
                if (intMeta && extMeta)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_BACKUP_CONFIG,
                            "Backup shipping of resource '" + rscDfn.getName().displayValue +
                                "' cannot be started since there is no support for mixing external and internal " +
                                "drbd-metadata among volumes."
                        )
                    );
                }
                if (!extMeta)
                {
                    for (DrbdRscData<Resource> rscData : drbdLayer.getRscDfnLayerObject().getDrbdRscDataList())
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
            }

            Snapshot createdSnapshot = snapDfn.getSnapshot(peerAccCtx.get(), chosenNodeName);
            if (setShippingFlag)
            {
                createdSnapshot.getFlags()
                    .enableFlags(peerAccCtx.get(), Snapshot.Flags.BACKUP_SOURCE);
            }
            Props snapProps = createdSnapshot.getProps(peerAccCtx.get());
            snapProps.setProp(
                InternalApiConsts.KEY_BACKUP_NODE_IDS_TO_RESET,
                StringUtils.join(nodeIds, InternalApiConsts.KEY_BACKUP_NODE_ID_SEPERATOR),
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
            snapProps.setProp(
                InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                remoteName,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );

            setIncrementalDependentProps(createdSnapshot, prevSnapDfn);

            ctrlTransactionHelper.commit();

            responses.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_INFO, "Shipping of resource " + rscNameRef + " to remote " + remoteName +
                        " in progress."
                ).putObjRef(ApiConsts.KEY_SNAPSHOT, snapName).build()
            );

            Flux<ApiCallRc> flux = snapshotCrtHandler.postCreateSnapshot(snapDfn)
                .concatWith(Flux.<ApiCallRc>just(responses));
            return new Pair<>(flux, createdSnapshot);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "start backup shpping", ApiConsts.FAIL_ACC_DENIED_RSC);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        catch (InvalidKeyException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * Gets the incremental backup base for the given resource. This checks for the last successful snapshot with a
     * matching backup shipping property.
     *
     * @param rscDfn the resource definition for which previous backups should be found.
     * @param remoteName The remote name, used to memorise previous snapshots.
     * @param allowIncremental If false, this will always return null, indicating a full backup should be created.
     *
     * @return The snapshot definition of the last snapshot uploaded to the given remote. Returns null if incremental
     * backups not allowed, no snapshot was found, or the found snapshot was not compatible.
     *
     * @throws AccessDeniedException when resource definition properties can't be accessed.
     * @throws InvalidNameException when detected previous snapshot name is invalid
     */
    private SnapshotDefinition getIncrementalBase(
        ResourceDefinition rscDfn,
        String remoteName,
        boolean allowIncremental
    )
        throws AccessDeniedException,
        InvalidNameException
    {
        SnapshotDefinition prevSnapDfn = null;
        if (allowIncremental)
        {
            String prevSnapName = rscDfn.getProps(peerAccCtx.get()).getProp(
                InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" + remoteName
            );

            if (prevSnapName != null)
            {
                prevSnapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscDfn, new SnapshotName(prevSnapName), false);
                if (prevSnapDfn == null)
                {
                    errorReporter.logWarning(
                        "Could not create an incremental backup for resource %s as the previous snapshot %s needed for the incremental backup has already been deleted. Creating a full backup instead.",
                        rscDfn.getName(),
                        prevSnapName
                    );
                }
                else
                {
                    for (SnapshotVolumeDefinition snapVlmDfn : prevSnapDfn.getAllSnapshotVolumeDefinitions(sysCtx))
                    {
                        long vlmDfnSize = snapVlmDfn.getVolumeDefinition().getVolumeSize(sysCtx);
                        long prevSnapVlmDfnSize = snapVlmDfn.getVolumeSize(sysCtx);
                        if (prevSnapVlmDfnSize != vlmDfnSize)
                        {
                            errorReporter.logDebug(
                                "Current vlmDfn size (%d) does not match with prev snapDfn (%s) size (%d). Forcing full backup.",
                                vlmDfnSize,
                                snapVlmDfn,
                                prevSnapVlmDfnSize
                            );
                            prevSnapDfn = null;
                        }
                    }
                }
            }
            else
            {
                errorReporter.logWarning(
                    "Could not create an incremental backup for resource %s as there is no previous full backup. Creating a full backup instead.",
                    rscDfn.getName()
                );
            }
        }

        return prevSnapDfn;
    }

    /**
     * In case we already shipped a backup (full or incremental), we need to make sure that the chosen node
     * also has that snapshot created, otherwise we are not able to send incremental backup.
     * This method could also be used to verify if the backed up DeviceProviderKind match with the node's snapshot
     * DeviceProviderKind, but as we are currently not supporting mixed DeviceProviderKinds we also neglect
     * this check here.
     *
     * @param rscDfn
     * @param prevSnapDfnRef
     * @param optionalExtToolsRef
     * @param requiredExtToolsRef
     *
     * @return
     *
     * @throws AccessDeniedException
     */
    private Pair<Node, List<String>> chooseNode(
        ResourceDefinition rscDfn,
        SnapshotDefinition prevSnapDfnRef,
        Map<ExtTools, ExtToolsInfo.Version> requiredExtToolsMap,
        Map<ExtTools, ExtToolsInfo.Version> optionalExtToolsMap
    )
        throws AccessDeniedException
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        Node selectedNode = null;
        Node preferredNode = null;
        List<String> nodeNamesStr = new ArrayList<>();

        Iterator<Resource> rscIt = rscDfn.iterateResource(peerAccCtx.get());
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            if (
                !rsc.getStateFlags()
                    .isSomeSet(peerAccCtx.get(), Resource.Flags.DRBD_DISKLESS, Resource.Flags.NVME_INITIATOR))
            {
                ApiCallRcImpl backupShippingSupported = backupShippingSupported(rsc);
                if (backupShippingSupported.isEmpty())
                {
                    Node node = rsc.getNode();
                    boolean takeSnapshot = true;
                    if (prevSnapDfnRef != null)
                    {
                        takeSnapshot = prevSnapDfnRef.getAllSnapshots(peerAccCtx.get()).stream()
                            .anyMatch(snap -> snap.getNode().equals(node));
                    }
                    if (!takeSnapshot)
                    {
                        apiCallRc.addEntries(
                            ApiCallRcImpl.singleApiCallRc(
                                ApiConsts.MASK_INFO,
                                "Cannot create snapshot on node '" + node.getName().displayValue +
                                    "', as the node does not have the required base snapshot for incremental backup"
                            )
                        );
                    }
                    else
                    {
                        takeSnapshot = hasNodeAllExtTools(
                            node,
                            requiredExtToolsMap,
                            apiCallRc,
                            "Cannot use node '" + node.getName().displayValue + "' as it does not support the tool(s): "
                        );
                    }

                    if (takeSnapshot)
                    {
                        if (selectedNode == null)
                        {
                            selectedNode = node;
                        }
                        if (preferredNode == null && hasNodeAllExtTools(node, optionalExtToolsMap, null, null))
                        {
                            preferredNode = node;
                            selectedNode = node;
                        }
                        nodeNamesStr.add(node.getName().displayValue);
                    }
                }
                else
                {
                    apiCallRc.addEntries(backupShippingSupported);
                }
            }
        }
        if (nodeNamesStr.size() == 0)
        {
            apiCallRc.addEntry(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_ENOUGH_NODES,
                    "Backup shipping of resource '" + rscDfn.getName().displayValue +
                        "' cannot be started since there is no node available that supports backup shipping."
                )
            );
            throw new ApiRcException(apiCallRc);
        }
        return new Pair<>(selectedNode, nodeNamesStr);
    }

    private boolean hasNodeAllExtTools(
        Node node,
        Map<ExtTools, ExtToolsInfo.Version> extTools,
        ApiCallRcImpl apiCallRcRef,
        String errorMsgPrefix
    )
        throws AccessDeniedException
    {
        boolean ret = true;
        if (extTools != null)
        {
            ExtToolsManager extToolsMgr = node.getPeer(sysCtx).getExtToolsManager();
            StringBuilder sb = new StringBuilder();
            for (Entry<ExtTools, Version> extTool : extTools.entrySet())
            {
                ExtToolsInfo extToolInfo = extToolsMgr.getExtToolInfo(extTool.getKey());
                Version requiredVersion = extTool.getValue();
                if (extToolInfo == null || !extToolInfo.isSupported() ||
                    (requiredVersion != null && !extToolInfo.hasVersionOrHigher(requiredVersion)))
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

    void setIncrementalDependentProps(Snapshot snap, SnapshotDefinition prevSnapDfn)
        throws InvalidValueException, AccessDeniedException, DatabaseException
    {
        SnapshotDefinition snapDfn = snap.getSnapshotDefinition();
        if (prevSnapDfn == null)
        {
            snapDfn.getProps(peerAccCtx.get())
                .setProp(
                    InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP,
                    snap.getSnapshotName().displayValue,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                );
        }
        else
        {
            snapDfn.getProps(peerAccCtx.get())
                .setProp(
                    InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP,
                    prevSnapDfn.getProps(peerAccCtx.get()).getProp(
                        InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    ),
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                );
            snap.getProps(peerAccCtx.get()).setProp(
                InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                prevSnapDfn.getName().displayValue,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
        }
    }

    public Flux<ApiCallRc> deleteBackup(
        String rscName,
        String id,
        String idPrefix,
        String timestamp,
        String nodeName,
        boolean cascading,
        boolean allLocalCluster,
        boolean all,
        String s3Key,
        String remoteName,
        boolean dryRun
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Delete backup",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> deleteBackupInTransaction(
                id,
                idPrefix,
                cascading,
                rscName,
                nodeName,
                timestamp,
                allLocalCluster,
                all,
                s3Key,
                remoteName,
                dryRun
            )
        );
    }

    private Flux<ApiCallRc> deleteBackupInTransaction(
        String id,
        String idPrefix,
        boolean cascading,
        String rscName,
        String nodeName,
        String timestamp,
        boolean allLocalCluster,
        boolean all,
        String s3Key,
        String remoteName,
        boolean dryRun
    ) throws AccessDeniedException, InvalidNameException
    {
        S3Remote s3Remote = getS3Remote(remoteName);
        ToDeleteCollections toDelete = new ToDeleteCollections();

        /*
         * Currently the following cases are allowed:
         * 1) id [cascading]
         * 2) idPrefix [cascading]
         * 3) s3Key [cascading]
         * 4) (time|rsc|node)+ [cascading]
         * 5) all // force cascading
         * 6) allCluster // forced cascading
         */
        List<S3ObjectSummary> objects = backupHandler.listObjects(
            null,
            s3Remote,
            peerAccCtx.get(),
            getLocalMasterKey()
        );
        // get ALL s3 keys of the given bucket, including possibly not linstor related ones
        Set<String> allS3Keys = objects.stream()
            .map(S3ObjectSummary::getKey)
            .collect(Collectors.toCollection(TreeSet::new));
        Map<String, S3ObjectInfo> s3LinstorObjects = loadAllLinstorS3Objects(
            allS3Keys,
            s3Remote,
            toDelete.apiCallRcs
        );
        if (id != null && !id.isEmpty()) // case 1: id [cascading]
        {
            final String delId = id.endsWith(META_SUFFIX) ? id : id + META_SUFFIX;
            deleteByIdPrefix(
                delId,
                false,
                cascading,
                s3LinstorObjects,
                s3Remote,
                toDelete
            );
        }
        else
        if (idPrefix != null && !idPrefix.isEmpty()) // case 2: idPrefix [cascading]
        {
            deleteByIdPrefix(
                idPrefix,
                true,
                cascading,
                s3LinstorObjects,
                s3Remote,
                toDelete
            );
        }
        else
        if (s3Key != null && !s3Key.isEmpty()) // case 3: s3Key [cascading]
        {
            deleteByS3Key(s3LinstorObjects, Collections.singleton(s3Key), cascading, toDelete);
            toDelete.s3keys.add(s3Key);
            toDelete.s3KeysNotFound.remove(s3Key); // ignore this
        }
        else
        if (
            timestamp != null && !timestamp.isEmpty() ||
                rscName != null && !rscName.isEmpty() ||
                nodeName != null && !nodeName.isEmpty()
        ) // case 4: (time|rsc|node)+ [cascading]
        {
            deleteByTimeRscNode(
                s3LinstorObjects,
                timestamp,
                rscName,
                nodeName,
                cascading,
                toDelete
            );
        }
        else
        if (all) // case 5: all // force cascading
        {
            deleteByS3Key(
                s3LinstorObjects,
                s3LinstorObjects.keySet(),
                true,
                toDelete
            );
        }
        else
        if (allLocalCluster) // case 6: allCluster // forced cascading
        {
            deleteAllLocalCluster(
                s3LinstorObjects,
                toDelete
            );
        }

        Flux<ApiCallRc> deleteSnapFlux = Flux.empty();
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        if (dryRun)
        {
            boolean nothingToDelete = true;
            if (!toDelete.s3keys.isEmpty())
            {
                StringBuilder sb = new StringBuilder("Would delete s3 objects:\n");
                nothingToDelete = false;
                for (String s3KeyToDelete : toDelete.s3keys)
                {
                    sb.append("  ").append(s3KeyToDelete).append("\n");
                }
                apiCallRc.addEntry(sb.toString(), 0); // retCode 0 as nothing actually happened..
            }
            if (!toDelete.snapKeys.isEmpty())
            {
                nothingToDelete = false;
                StringBuilder sb = new StringBuilder("Would delete Snapshots:\n");
                for (SnapshotDefinition.Key snapKey : toDelete.snapKeys)
                {
                    sb.append("  Resource: ").append(snapKey.getResourceName().displayValue).append(", Snapshot: ")
                        .append(snapKey.getSnapshotName().displayValue).append("\n");
                }
                apiCallRc.addEntry(sb.toString(), 0); // retCode 0 as nothing actually happened..
            }
            if (nothingToDelete)
            {
                // retCode 0 as nothing actually happened..
                apiCallRc.addEntry("Dryrun mode. Although nothing selected for deletion", 0);
            }
        }
        else
        {
            for (SnapshotDefinition.Key snapKey : toDelete.snapKeys)
            {
                deleteSnapFlux = deleteSnapFlux.concatWith(
                    ctrlSnapDeleteApiCallHandler.deleteSnapshot(
                        snapKey.getResourceName().displayValue,
                        snapKey.getSnapshotName().displayValue,
                        null
                    )
                );
            }
            try
            {
                if (!toDelete.s3keys.isEmpty())
                {
                    backupHandler.deleteObjects(toDelete.s3keys, s3Remote, peerAccCtx.get(), getLocalMasterKey());
                }
                else
                {
                    apiCallRc.addEntry(
                        "Could not find any backups to delete.",
                        ApiConsts.FAIL_INVLD_REQUEST | ApiConsts.MASK_BACKUP
                    );
                    return Flux.just(apiCallRc);
                }
            }
            catch (MultiObjectDeleteException exc)
            {
                Set<String> deletedKeys = new TreeSet<>();
                for (DeletedObject obj : exc.getDeletedObjects())
                {
                    deletedKeys.add(obj.getKey());
                }
                toDelete.s3keys.removeAll(deletedKeys);
                apiCallRc.addEntry(
                    "Could not delete " + toDelete.s3keys.toString(),
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP
                );
                toDelete.s3keys = deletedKeys;
            }
            apiCallRc.addEntry(
                "Successfully deleted " + toDelete.s3keys.toString(),
                ApiConsts.MASK_SUCCESS | ApiConsts.MASK_BACKUP
            );
        }
        if (!toDelete.s3KeysNotFound.isEmpty())
        {
            StringBuilder sb = new StringBuilder("The following S3 keys were not found in the given remote:\n");
            for (String s3KeyNotFound : toDelete.s3KeysNotFound)
            {
                sb.append("  ").append(s3KeyNotFound).append("\n");
            }
            apiCallRc.addEntry(sb.toString(), ApiConsts.WARN_NOT_FOUND);
        }

        apiCallRc.addEntries(toDelete.apiCallRcs);
        return Flux.<ApiCallRc> just(apiCallRc).concatWith(deleteSnapFlux);
    }

    private void deleteByIdPrefix(
        String idPrefixRef,
        boolean allowMultiSelectionRef,
        boolean cascadingRef,
        Map<String, S3ObjectInfo> s3LinstorObjects,
        S3Remote s3RemoteRef,
        ToDeleteCollections toDeleteRef
    )
        throws AccessDeniedException
    {
        TreeSet<String> matchingS3Keys = new TreeSet<>();
        for (String s3Key : s3LinstorObjects.keySet())
        {
            if (s3Key.startsWith(idPrefixRef))
            {
                matchingS3Keys.add(s3Key);
            }
        }
        int s3KeyCount = matchingS3Keys.size();
        if (s3KeyCount == 0)
        {
            toDeleteRef.apiCallRcs.addEntry(
                "No backup with id " + (allowMultiSelectionRef ? "prefix " : "") + "'" + idPrefixRef +
                    "' found on remote '" +
                    s3RemoteRef.getName().displayValue + "'",
                ApiConsts.WARN_NOT_FOUND
            );
        }
        else
        {
            if (s3KeyCount > 1 && !allowMultiSelectionRef)
            {
                StringBuilder sb = new StringBuilder("Ambigious id '");
                sb.append(idPrefixRef).append("' for remote '").append(s3RemoteRef.getName().displayValue)
                    .append("':\n");
                for (String s3Key : matchingS3Keys)
                {
                    sb.append("  ").append(s3Key).append("\n");
                }
                toDeleteRef.apiCallRcs.addEntry(
                    sb.toString(),
                    ApiConsts.FAIL_NOT_FOUND_BACKUP
                );
            }
            else
            {
                deleteByS3Key(s3LinstorObjects, matchingS3Keys, cascadingRef, toDeleteRef);
            }
        }
    }

    private void deleteByS3Key(
        Map<String, S3ObjectInfo> s3LinstorObjects,
        Set<String> s3KeysToDeleteRef,
        boolean cascadingRef,
        ToDeleteCollections toDeleteRef
    )
        throws AccessDeniedException
    {
        for (String s3Key : s3KeysToDeleteRef)
        {
            S3ObjectInfo s3ObjectInfo = s3LinstorObjects.get(s3Key);
            if (s3ObjectInfo != null && s3ObjectInfo.exists)
            {
                addToDeleteList(s3LinstorObjects, s3ObjectInfo, cascadingRef, toDeleteRef);
            }
            else
            {
                toDeleteRef.s3KeysNotFound.add(s3Key);
            }
        }
    }

    private static void addToDeleteList(
        Map<String, S3ObjectInfo> s3Map,
        S3ObjectInfo s3ObjectInfo,
        boolean cascading,
        ToDeleteCollections toDelete
    )
    {
        if (s3ObjectInfo.isMetaFile())
        {
            toDelete.s3keys.add(s3ObjectInfo.s3Key);
            for (S3ObjectInfo childObj : s3ObjectInfo.references)
            {
                if (childObj.exists)
                {
                    if (!childObj.isMetaFile())
                    {
                        toDelete.s3keys.add(childObj.s3Key);
                    }
                    // we do not want to cascade upwards. only delete child / data keys
                }
                else
                {
                    toDelete.s3KeysNotFound.add(childObj.s3Key);
                }
            }
            for (S3ObjectInfo childObj : s3ObjectInfo.referencedBy)
            {
                if (childObj.exists)
                {
                    if (childObj.isMetaFile())
                    {
                        if (cascading)
                        {
                            addToDeleteList(s3Map, childObj, cascading, toDelete);
                        }
                        else
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl.simpleEntry(
                                    ApiConsts.FAIL_DEPENDEND_BACKUP,
                                    s3ObjectInfo.s3Key + " should be deleted, but at least " + childObj.s3Key +
                                        " is referencing it. Use --cascading to delete recursively"
                                )
                            );
                        }
                    }
                    // we should not be referenced by something other than a metaFile
                }
                else
                {
                    toDelete.s3KeysNotFound.add(childObj.s3Key);
                }
            }
            if (s3ObjectInfo.snapDfn != null)
            {
                toDelete.snapKeys.add(new SnapshotDefinition.Key(s3ObjectInfo.snapDfn));
            }
        }
    }

    private void deleteByTimeRscNode(
        Map<String, S3ObjectInfo> s3LinstorObjectsRef,
        String timestampRef,
        String rscNameRef,
        String nodeNameRef,
        boolean cascadingRef,
        ToDeleteCollections toDeleteRef
    )
        throws AccessDeniedException
    {
        Predicate<String> nodeNameCheck = nodeNameRef == null ||
            nodeNameRef.isEmpty() ? ignore -> true : nodeNameRef::equalsIgnoreCase;
        Predicate<String> rscNameCheck = rscNameRef == null ||
            rscNameRef.isEmpty() ? ignore -> true : rscNameRef::equalsIgnoreCase;
        Predicate<Long> timestampCheck;
        if (timestampRef == null || timestampRef.isEmpty())
        {
            timestampCheck = ignore -> true;
        }
        else
        {
            try
            {
                Date date = S3Consts.DATE_FORMAT.parse(timestampRef);
                timestampCheck = timestamp -> date.after(new Date(timestamp));
            }
            catch (ParseException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_TIME_PARAM,
                        "Failed to parse '" + timestampRef +
                            "'. Expected format: YYYYMMDD_HHMMSS (e.g. 20210824_072543)"
                    ),
                    exc
                );
            }
        }
        TreeSet<String> s3KeysToDelete = new TreeSet<>();
        for (S3ObjectInfo s3Obj : s3LinstorObjectsRef.values())
        {
            if (s3Obj.isMetaFile())
            {
                String node = s3Obj.metaFile.getNodeName();
                String rsc = s3Obj.metaFile.getRscName();
                long startTimestamp;
                try
                {
                    S3MetafileNameInfo meta = new S3MetafileNameInfo(s3Obj.s3Key);
                    startTimestamp = meta.backupTime.getTime();
                }
                catch (ParseException exc)
                {
                    throw new ImplementationError("Invalid meta file name", exc);
                }
                if (nodeNameCheck.test(node) && rscNameCheck.test(rsc) && timestampCheck.test(startTimestamp))
                {
                    s3KeysToDelete.add(s3Obj.s3Key);
                }
            }
        }
        deleteByS3Key(s3LinstorObjectsRef, s3KeysToDelete, cascadingRef, toDeleteRef);
    }

    private void deleteAllLocalCluster(
        Map<String, S3ObjectInfo> s3LinstorObjectsRef,
        ToDeleteCollections toDeleteRef
    )
        throws InvalidKeyException, AccessDeniedException
    {
        String localClusterId = sysCfgRepo.getCtrlConfForView(peerAccCtx.get()).getProp(LinStor.PROP_KEY_CLUSTER_ID);
        Set<String> s3KeysToDelete = new TreeSet<>();
        for (S3ObjectInfo s3Obj : s3LinstorObjectsRef.values())
        {
            if (s3Obj.metaFile != null && localClusterId.equals(s3Obj.metaFile.getClusterId()))
            {
                s3KeysToDelete.add(s3Obj.s3Key);
            }
        }
        deleteByS3Key(s3LinstorObjectsRef, s3KeysToDelete, true, toDeleteRef);
    }

    private Map<String, S3ObjectInfo> loadAllLinstorS3Objects(
        Set<String> allS3KeyRef,
        S3Remote s3RemoteRef,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException
    {
        Map<String, S3ObjectInfo> ret = new TreeMap<>();
        // add all backups to the list that have usable metadata-files
        for (String s3Key : allS3KeyRef)
        {
            try
            {
                S3MetafileNameInfo info = new S3MetafileNameInfo(s3Key);
                // throws parse exception if not linstor json
                BackupMetaDataPojo s3MetaFile = backupHandler.getMetaFile(
                    s3Key,
                    s3RemoteRef,
                    peerAccCtx.get(),
                    getLocalMasterKey()
                );
                S3ObjectInfo metaInfo = ret.computeIfAbsent(s3Key, S3ObjectInfo::new);
                metaInfo.exists = true;
                metaInfo.metaFile = s3MetaFile;
                for (List<BackupMetaInfoPojo> backupInfoPojoList : s3MetaFile.getBackups().values())
                {
                    for (BackupMetaInfoPojo backupInfoPojo : backupInfoPojoList)
                    {
                        String childS3Key = backupInfoPojo.getName();
                        S3ObjectInfo childS3Obj = ret.computeIfAbsent(childS3Key, S3ObjectInfo::new);
                        childS3Obj.referencedBy.add(metaInfo);
                        metaInfo.references.add(childS3Obj);
                    }
                }

                SnapshotDefinition snapDfn = loadSnapDfnIfExists(info.rscName, info.snapName);
                if (snapDfn != null)
                {
                    if (snapDfn.getUuid().toString().equals(s3MetaFile.getSnapDfnUuid()))
                    {
                        metaInfo.snapDfn = snapDfn;
                    }
                    else
                    {
                        apiCallRc.addEntry("Not marking SnapshotDefinition " +info.rscName +" / " +info.snapName +" for exclusion as the UUID does not match with the backup",ApiConsts.WARN_NOT_FOUND);
                    }
                }

                String basedOnS3Key = s3MetaFile.getBasedOn();
                if (basedOnS3Key != null)
                {
                    S3ObjectInfo basedOnS3MetaInfo = ret.computeIfAbsent(basedOnS3Key, S3ObjectInfo::new);
                    basedOnS3MetaInfo.referencedBy.add(metaInfo);
                    metaInfo.references.add(basedOnS3MetaInfo);
                }
            }
            catch (MismatchedInputException exc)
            {
                // ignore, most likely an older format of linstor's backup .meta json
            }
            catch (IOException exc)
            {
                String errRepId = errorReporter.reportError(exc, peerAccCtx.get(), null, "used s3 key: " + s3Key);
                apiCallRc.addEntry(
                    "IO exception while parsing metafile " + s3Key + ". Details in error report " + errRepId,
                    ApiConsts.FAIL_UNKNOWN_ERROR
                );
            }
            catch (ParseException ignored)
            {
                // Ignored, not a meta file
            }


            try
            {
                S3VolumeNameInfo info = new S3VolumeNameInfo(s3Key);
                S3ObjectInfo s3DataInfo = ret.computeIfAbsent(s3Key, S3ObjectInfo::new);
                s3DataInfo.exists = true;
                s3DataInfo.snapDfn = loadSnapDfnIfExists(info.rscName, info.snapName);
            }
            catch (ParseException ignored)
            {
                // Ignored, not a volume file
            }
        }

        return ret;
    }

    public Flux<ApiCallRc> restoreBackup(
        String srcRscName,
        String srcSnapName,
        String backupId,
        Map<String, String> storPoolMapRef,
        String nodeName,
        String targetRscName,
        String remoteName,
        String passphrase,
        boolean downloadOnly
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
                        remoteName,
                        passphrase,
                        downloadOnly
                    )
                )
            );
    }

    private Flux<ApiCallRc> restoreBackupInTransaction(
        String srcRscName,
        String srcSnapName,
        String backupId,
        Map<String, String> storPoolMap,
        String nodeName,
        String targetRscName,
        String remoteName,
        String passphrase,
        boolean downloadOnly
    ) throws AccessDeniedException, InvalidNameException
    {
        // 1. Ensure node is ready
        Node node = ctrlApiDataLoader.loadNode(nodeName, true);
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
        S3Remote remote = getS3Remote(remoteName);
        byte[] targetMasterKey = getLocalMasterKey();
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
            s3keys = objects.stream().map(S3ObjectSummary::getKey).collect(Collectors.toCollection(TreeSet::new));
            toRestore = getLatestBackup(s3keys, srcSnapName);
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
            boolean resetData = true; // reset data based on the final snapshot
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

            // 3b. check if given storpools have enough space remaining for the restore
            boolean first = true;
            for (Pair<S3MetafileNameInfo, BackupMetaDataPojo> meta : metadataChain)
            {
                Pair<Long, Long> totalSizes = new Pair<>(0L, 0L); // dlSize, allocSize
                fillBackupInfo(first, storPoolInfo, objects, meta.objB, meta.objB.getLayerData(), totalSizes);
                first = false;
            }
            Map<String, Long> remainingFreeSpace = getRemainingSize(storPoolInfo, storPoolMap, nodeName);
            List<String> spTooFull = new ArrayList<>();
            for (Entry<String, Long> spaceEntry : remainingFreeSpace.entrySet())
            {
                if (spaceEntry != null && spaceEntry.getValue() < 0)
                {
                    spTooFull.add(
                        ctrlApiDataLoader.loadStorPool(spaceEntry.getKey(), nodeName, true).getName().displayValue
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
            for (Pair<S3MetafileNameInfo, BackupMetaDataPojo> metadata : metadataChain)
            {
                Snapshot snap = createSnapshotByS3Meta(
                    metadata.objA,
                    storPoolMap,
                    node,
                    targetRscName,
                    passphrase,
                    remote,
                    s3keys,
                    metadata.objB,
                    responses,
                    resetData,
                    downloadOnly
                );
                backupInfoMgr.abortRestoreAddEntry(targetRscName, snap);
                // all other "basedOn" snapshots should not change props / size / etc..
                resetData = false;
                snap.getProps(peerAccCtx.get()).setProp(
                    InternalApiConsts.KEY_BACKUP_TO_RESTORE,
                    metadata.objA.toString(),
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                );
                if (nextBackup != null)
                {
                    backupInfoMgr.backupsToDownloadAddEntry(snap, nextBackup);
                }
                nextBackup = snap;
            }

            // 3d. nextBackup now points to the end of the chain, i.e. the full backup. Start restoring from here
            nextBackup.getFlags().enableFlags(peerAccCtx.get(), Snapshot.Flags.BACKUP_TARGET);
            if (!backupInfoMgr.restoreAddEntry(nextBackup.getResourceDefinition(), toRestore.toString()))
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
            return snapshotCrtHandler.postCreateSnapshot(nextBackSnapDfn)
                .concatWith(Flux.just(responses))
                .onErrorResume(error -> cleanupAfterFailedRestore(error, nextBackSnapDfn));
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

    private S3MetafileNameInfo getLatestBackup(Set<String> s3keys, String snapName)
    {
        S3MetafileNameInfo latest = null;
        for (String key : s3keys)
        {
            try
            {
                S3MetafileNameInfo current = new S3MetafileNameInfo(key);
                if (snapName != null && !snapName.isEmpty() && !snapName.equals(current.snapName))
                {
                    // Snapshot names do not match, ignore this backup
                    continue;
                }
                if (latest == null || latest.backupTime.before(current.backupTime))
                {
                    latest = current;
                }
            }
            catch (ParseException ignored)
            {
                // Not a metadata file, ignore
            }
        }
        return latest;
    }

    private Snapshot createSnapshotByS3Meta(
        S3MetafileNameInfo metafileNameInfo,
        Map<String, String> storPoolMap,
        Node node,
        String targetRscName,
        String passphrase,
        S3Remote remote,
        Set<String> s3keys,
        BackupMetaDataPojo metadata,
        ApiCallRcImpl responses,
        boolean resetData,
        boolean downloadOnly
    )
        throws AccessDeniedException, ImplementationError, DatabaseException, InvalidValueException
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
        ResourceDefinition rscDfn = getRscDfnForBackupRestore(targetRscName, snapName, metadata, false, resetData);
        SnapshotDefinition snapDfn = getSnapDfnForBackupRestore(metadata, snapName, rscDfn, responses, downloadOnly);
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
            remote
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

    @SuppressWarnings("unchecked")
    Flux<BackupShippingStartInfo> restoreBackupL2LInTransaction(
        Map<StorPool.Key, Long> thinFreeCapacities,
        CtrlBackupL2LDstApiCallHandler.BackupShippingData data
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        Flux<BackupShippingStartInfo> ret;

        try
        {
            // 5. create layerPayload
            RscLayerDataApi layers = data.metaData.getLayerData();

            // 6. do luks-stuff if needed (is converted by L2L-source)

            // search for incremental base
            Snapshot incrementalBaseSnap = null;
            boolean resetData = false;
            try
            {
                /*
                 * DO NOT use getRscDfnForBackupRestore here. We need to know if we are choosing incremental or full
                 * backup before calling
                 */
                ResourceDefinition targetRscDfn = rscDfnRepo.get(sysCtx, new ResourceName(data.dstRscName));
                if (targetRscDfn != null)
                {
                    incrementalBaseSnap = getIncrementalBase(
                        targetRscDfn,
                        data.srcSnapDfnUuids
                    );
                }
                resetData = targetRscDfn == null || targetRscDfn.getResourceCount() == 0;
            }
            catch (InvalidNameException exc)
            {
                throw new ImplementationError(exc);
            }
            // 8. create rscDfn
            ResourceDefinition rscDfn = getRscDfnForBackupRestore(
                data.dstRscName,
                data.snapName,
                data.metaData,
                true,
                resetData
            );
            SnapshotDefinition snapDfn;
            Map<Integer, SnapshotVolumeDefinition> snapVlmDfns = new TreeMap<>();
            Set<StorPool> storPools;
            Snapshot snap = null;

            // 9. create snapDfn
            snapDfn = getSnapDfnForBackupRestore(data.metaData, data.snapName, rscDfn, responses, data.downloadOnly);
            // 10. create vlmDfn(s)
            // 11. create snapVlmDfn(s)
            long totalSize = createSnapVlmDfnForBackupRestore(
                data.dstRscName,
                data.metaData,
                rscDfn,
                snapDfn,
                snapVlmDfns,
                resetData
            );

            if (incrementalBaseSnap != null)
            {
                for (SnapshotVolumeDefinition snapVlmDfn : snapDfn.getAllSnapshotVolumeDefinitions(sysCtx))
                {
                    snapVlmDfns.put(snapVlmDfn.getVolumeNumber().value, snapVlmDfn);
                }

                data.metaData.getRsc().getProps().put(
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
                if (data.useZstd)
                {
                    extTools.put(ExtTools.ZSTD, null);
                }
                AutoSelectFilterBuilder autoSelectBuilder = new AutoSelectFilterBuilder()
                    .setPlaceCount(1)
                    .setNodeNameList(data.dstNodeName == null ? null : Arrays.asList(data.dstNodeName))
                    .setStorPoolNameList(data.dstStorPool == null ? null : Arrays.asList(data.dstStorPool))
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
                if ((storPools == null || storPools.isEmpty()) && data.useZstd)
                {
                    // try not using it..
                    data.useZstd = false;
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
                    data.metaData,
                    layers,
                    node,
                    snapDfn,
                    snapVlmDfns,
                    data.storPoolRenameMap,
                    data.stltRemote
                );
                snap.getProps(peerAccCtx.get()).setProp(
                    InternalApiConsts.KEY_BACKUP_TO_RESTORE,
                    snap.getResourceName().displayValue + "_" + snap.getSnapshotName().displayValue,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                );
                snap.getFlags().enableFlags(peerAccCtx.get(), Snapshot.Flags.BACKUP_TARGET);
                backupInfoMgr.abortRestoreAddEntry(data.dstRscName, snap);

                // LUKS
                Set<AbsRscLayerObject<Snapshot>> luksLayerData = LayerRscUtils.getRscDataByProvider(
                    snap.getLayerData(sysCtx),
                    DeviceLayerKind.LUKS
                );
                if (!luksLayerData.isEmpty())
                {
                    LuksLayerMetaPojo luksInfo = data.metaData.getLuksInfo();
                    if (luksInfo == null)
                    {
                        throw new ImplementationError("Cannot receive LUKS data without LuksInfo");
                    }
                    byte[] remoteMasterKey = getRemoteMasterKey(data.srcClusterId, luksInfo);
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

                // remoteMasterKey = getRemoteMasterKey(sourceClusterIdStr, luksInfo);

                data.stltRemote.useZstd(sysCtx, data.useZstd);

                // update stlts
                ctrlTransactionHelper.commit();

                String srcSnapDfnUuid = null; // if not null => incremental sync
                Snapshot incrBaseSnap = incrementalBaseSnap;
                try
                {
                    if (incrBaseSnap != null)
                    {
                        srcSnapDfnUuid = incrBaseSnap.getSnapshotDefinition().getProps(sysCtx).getProp(
                            InternalApiConsts.KEY_BACKUP_L2L_SRC_SNAP_DFN_UUID
                        );
                    }
                }
                catch (AccessDeniedException exc)
                {
                    throw new ImplementationError(exc);
                }
                data.incrBaseSnapDfnUuid = srcSnapDfnUuid;
                backupInfoMgr.addL2LDstData(snap, data);

                // calling ...SupressingErrorClasses without classes => do not ignore DelayedApiRcException. we want to
                // deal with that by our self
                ret = ctrlSatelliteUpdateCaller.updateSatellites(data.stltRemote)
                    .thenMany(
                        snapshotCrtHandler.postCreateSnapshotSuppressingErrorClasses(snapDfn)
                            .onErrorResume(error -> cleanupAfterFailedRestore(error, snapDfn))
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
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    private byte[] getRemoteMasterKey(String sourceClusterIdStr, LuksLayerMetaPojo luksInfo)
        throws AccessDeniedException
    {
        byte[] remoteMasterKey = null;
        UUID srcClusterId = UUID.fromString(sourceClusterIdStr);
        for (Remote sourceRemote : remoteRepo.getMapForView(sysCtx).values())
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
                        byte[] remotePassphrase = decHelper.decrypt(getLocalMasterKey(), encryptedRemotePassphrase);
                        remoteMasterKey = encHelper.getDecryptedMasterKey(
                            luksInfo.getMasterCryptHash(),
                            luksInfo.getMasterPassword(),
                            luksInfo.getMasterCryptSalt(),
                            new String(remotePassphrase, Charset.forName("UTF-8"))
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

    private Snapshot getIncrementalBase(
        ResourceDefinition rscDfnRef,
        Set<String> srcSnapDfnUuidsForIncrementalRef
    )
        throws AccessDeniedException
    {
        Snapshot ret = null;

        long latestTimestamp = -1;
        for (SnapshotDefinition snapDfn : rscDfnRef.getSnapshotDfns(sysCtx))
        {
            String fromSrcSnapDfnUuid = snapDfn.getProps(sysCtx).getProp(
                InternalApiConsts.KEY_BACKUP_L2L_SRC_SNAP_DFN_UUID
            );
            if (srcSnapDfnUuidsForIncrementalRef.contains(fromSrcSnapDfnUuid))
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
                 * snapshotDfn will have only one snapshot (we will certainly not have received a backup into multiple
                 * nodes)
                 */
                Snapshot snap = snapshotIterator.next();
                long timestamp = Long.parseLong(
                    snapDfn.getProps(sysCtx).getProp(
                        InternalApiConsts.KEY_BACKUP_START_TIMESTAMP,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    )
                );
                if (timestamp > latestTimestamp)
                {
                    latestTimestamp = timestamp;
                    ret = snap;
                }
            }
        }
        return ret;
    }

    private Flux<ApiCallRc> cleanupAfterFailedRestore(Throwable throwable, SnapshotDefinition snapDfnRef)
    {
        return scopeRunner.fluxInTransactionalScope(
            "cleanup after failed restore", lockGuardFactory.buildDeferred(
                LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP
            ),
            () -> cleanupAfterFailedRestoreInTransaction(
                throwable,
                snapDfnRef
            )
        );
    }

    private Flux<ApiCallRc> cleanupAfterFailedRestoreInTransaction(
        Throwable throwableRef,
        SnapshotDefinition snapDfnRef
    ) throws AccessDeniedException
    {
        for (Snapshot snap : snapDfnRef.getAllSnapshots(sysCtx))
        {
            backupInfoMgr.backupsToDownloadCleanUp(snap);
        }
        backupInfoMgr.restoreRemoveEntry(snapDfnRef.getResourceDefinition());
        ctrlTransactionHelper.commit();

        return Flux.error(throwableRef);
    }

    private long getAllocatedSizeSum(Snapshot snapRef)
    {
        long allocatedSizeSum = 0;
        try
        {
            Set<AbsRscLayerObject<Snapshot>> rscDataByProvider = LayerRscUtils
                .getRscDataByProvider(snapRef.getLayerData(sysCtx), DeviceLayerKind.STORAGE);
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

    private Snapshot createSnapshotAndVolumesForBackupRestore(
        BackupMetaDataPojo metadata,
        RscLayerDataApi layers,
        Node node,
        SnapshotDefinition snapDfn,
        Map<Integer, SnapshotVolumeDefinition> snapVlmDfns,
        Map<String, String> renameMap,
        Remote remote
    )
        throws AccessDeniedException, DatabaseException
    {
        Snapshot snap = snapshotCrtHelper
            .restoreSnapshot(snapDfn, node, layers, renameMap);
        Props snapProps = snap.getProps(peerAccCtx.get());

        snapProps.map().putAll(metadata.getRsc().getProps());
        try
        {
            snapProps.setProp(
                InternalApiConsts.KEY_BACKUP_SRC_REMOTE,
                remote.getName().displayValue,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );

            List<DeviceLayerKind> usedDeviceLayerKinds = LayerUtils.getUsedDeviceLayerKinds(
                snap.getLayerData(peerAccCtx.get()),
                peerAccCtx.get()
            );
            usedDeviceLayerKinds.removeAll(
                node.getPeer(peerAccCtx.get())
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
                    .restoreSnapshotVolume(layers, snap, snapVlmDfns.get(vlmMetaEntry.getKey()), renameMap);
                snapVlm.getProps(peerAccCtx.get()).map()
                    .putAll(metadata.getRsc().getVlms().get(vlmMetaEntry.getKey()).getProps());
            }
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        return snap;
    }

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
                vlmDfn.setVolumeSize(peerAccCtx.get(), vlmDfnMetaEntry.getValue().getSize());
            }
            vlmDfn.getProps(peerAccCtx.get()).map().putAll(vlmDfnMetaEntry.getValue().getProps());
            totalSize += vlmDfnMetaEntry.getValue().getSize();
            SnapshotVolumeDefinition snapVlmDfn = snapshotCrtHelper.createSnapshotVlmDfnData(snapDfn, vlmDfn);
            snapVlmDfn.getProps(peerAccCtx.get()).map().putAll(vlmDfnMetaEntry.getValue().getProps());
            snapVlmDfns.put(vlmDfnMetaEntry.getKey(), snapVlmDfn);
        }
        return totalSize;
    }

    private SnapshotDefinition getSnapDfnForBackupRestore(
        BackupMetaDataPojo metadata,
        SnapshotName snapName,
        ResourceDefinition rscDfn,
        ApiCallRcImpl responsesRef,
        boolean downloadOnly
    )
        throws AccessDeniedException, DatabaseException
    {
        SnapshotDefinition snapDfn = snapshotCrtHelper.createSnapshotDfnData(
            rscDfn,
            snapName,
            new SnapshotDefinition.Flags[] {}
        );
        snapDfn.getProps(peerAccCtx.get()).clear();
        snapDfn.getProps(peerAccCtx.get()).map().putAll(metadata.getRscDfn().getProps());

        // force the node to become primary afterwards in case we needed to recreate
        // the metadata
        snapDfn.getProps(peerAccCtx.get()).removeProp(InternalApiConsts.PROP_PRIMARY_SET);

        snapDfn.getFlags().enableFlags(
            peerAccCtx.get(),
            SnapshotDefinition.Flags.SHIPPING,
            SnapshotDefinition.Flags.BACKUP
        );

        if (rscDfn.getResourceCount() != 0)
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
        else if (!downloadOnly)
        {
            snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.RESTORE_BACKUP_ON_SUCCESS);
        }

        return snapDfn;
    }

    private ResourceDefinition getRscDfnForBackupRestore(
        String targetRscName,
        SnapshotName snapName,
        BackupMetaDataPojo metadata,
        boolean isL2L,
        boolean resetData
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
                    null,
                    true,
                    apiCallRcs,
                    false
                );
            }
            else if (rscDfn.getSnapshotDfn(peerAccCtx.get(), snapName) != null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN | ApiConsts.MASK_BACKUP,
                        "Snapshot " + snapName.displayValue + " already exists, please use snapshot restore instead."
                    )
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
            if (isL2L)
            {
                backupInfoMgr.restoreAddEntry(rscDfn, "");
            }
            if (resetData)
            {
                rscDfn.getFlags()
                    .resetFlagsTo(
                        peerAccCtx.get(),
                        ResourceDefinition.Flags.restoreFlags(metadata.getRscDfn().getFlags())
                    );
                rscDfn.getProps(peerAccCtx.get()).clear();
                rscDfn.getProps(peerAccCtx.get()).map().putAll(metadata.getRscDfn().getProps());

                // force the node to become primary afterwards in case we needed to recreate
                // the metadata
                rscDfn.getProps(peerAccCtx.get()).removeProp(InternalApiConsts.PROP_PRIMARY_SET);
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
        return rscDfn;
    }

    /**
     * @return
     * <code>Pair.objA</code>: Map of s3Key -> backupApi <br />
     * <code>Pair.objB</code>: Set of s3Keys that are either corrupt metafiles or not >
     */
    public Pair<Map<String, BackupApi>, Set<String>> listBackups(
        String rscNameRef,
        String snapNameRef,
        String remoteNameRef
    )
        throws AccessDeniedException, InvalidNameException
    {
        S3Remote remote = getS3Remote(remoteNameRef);
        AccessContext peerCtx = peerAccCtx.get();

        List<S3ObjectSummary> objects = backupHandler.listObjects(
            rscNameRef,
            remote,
            peerCtx,
            getLocalMasterKey()
        );
        // get ALL s3 keys of the given bucket, including possibly not linstor related ones
        Set<String> s3keys = objects.stream()
            .map(S3ObjectSummary::getKey)
            .collect(Collectors.toCollection(TreeSet::new));

        Map<String, BackupApi> retIdToBackupsApiMap = new TreeMap<>();

        /*
         * helper map. If we have "full", "inc1" (based on "full"), "inc2" (based on "inc1"), "inc3" (also based on
         * "full", i.e. if user deleted local inc1+inc2 before creating inc3)
         * This map will look like follows:
         * "" -> [full]
         * "full" -> [inc1, inc3]
         * "inc1" -> [inc2]
         * "" is a special id for full backups
         */
        Map<String, List<BackupApi>> idToUsedByBackupApiMap = new TreeMap<>();

        Set<String> linstorBackupsS3Keys = new TreeSet<>();

        // add all backups to the list that have useable metadata-files
        for (String s3key : s3keys)
        {
            try
            {
                S3MetafileNameInfo info = new S3MetafileNameInfo(s3key);
                if (snapNameRef != null && !snapNameRef.isEmpty() && !snapNameRef.equals(info.snapName))
                {
                    // Doesn't match the requested snapshot name, skip it.
                    continue;
                }

                Pair<BackupApi, Set<String>> result = getBackupFromMetadata(peerCtx, s3key, info, remote, s3keys);
                BackupApi back = result.objA;
                retIdToBackupsApiMap.put(back.getId(), back);
                linstorBackupsS3Keys.add(s3key);
                linstorBackupsS3Keys.addAll(result.objB);
                String base = back.getBasedOnId();
                if (base != null && !base.isEmpty())
                {
                    List<BackupApi> usedByList = idToUsedByBackupApiMap.computeIfAbsent(base, s -> new ArrayList<>());
                    usedByList.add(back);
                }
            }
            catch (MismatchedInputException exc)
            {
                errorReporter.logWarning(
                    "Could not parse metafile %s. Possibly created with older Linstor version",
                    s3key
                );
            }
            catch (IOException exc)
            {
                errorReporter.reportError(exc, peerCtx, null, "used s3 key: " + s3key);
            }
            catch (ParseException ignored)
            {
                // Ignored, wrong S3 key format
            }
        }
        s3keys.removeAll(linstorBackupsS3Keys);
        linstorBackupsS3Keys.clear();

        // add all backups to the list that look like backups, and maybe even have a rscDfn/snapDfn, but are not in a
        // metadata-file
        for (String s3key : s3keys)
        {
            if (!linstorBackupsS3Keys.contains(s3key))
            {
                try
                {
                    S3VolumeNameInfo info = new S3VolumeNameInfo(s3key);
                    if (snapNameRef != null && !snapNameRef.isEmpty() && !snapNameRef.equals(info.snapName))
                    {
                        continue;
                    }
                    SnapshotDefinition snapDfn = loadSnapDfnIfExists(info.rscName, info.snapName);

                    BackupApi back = getBackupFromVolumeKey(info, s3keys, linstorBackupsS3Keys, snapDfn);

                    retIdToBackupsApiMap.put(s3key, back);
                    linstorBackupsS3Keys.add(s3key);
                }
                catch (ParseException ignore)
                {
                    // ignored, not a volume file
                }
            }
        }
        // also check local snapDfns is anything is being uploaded but not yet visible in the s3 list (an upload might
        // only be shown in the list when it is completed)
        for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView(peerCtx).values())
        {
            for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(peerCtx))
            {
                String rscName = rscDfn.getName().displayValue;
                String snapName = snapDfn.getName().displayValue;

                String s3Suffix = snapDfn.getProps(peerCtx).getProp(
                    ApiConsts.KEY_BACKUP_S3_SUFFIX,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                );

                StateFlags<SnapshotDefinition.Flags> snapDfnFlags = snapDfn.getFlags();
                if (snapDfnFlags.isSet(peerCtx, SnapshotDefinition.Flags.BACKUP) &&
                    // ignore already shipped backups
                        !snapDfnFlags.isSet(peerCtx, SnapshotDefinition.Flags.SUCCESSFUL)
                )
                {
                    String backupTimeRaw = snapDfn.getProps(peerCtx)
                        .getProp(InternalApiConsts.KEY_BACKUP_START_TIMESTAMP, ApiConsts.NAMESPC_BACKUP_SHIPPING);

                    Date backupTime = new Date(Long.parseLong(backupTimeRaw));

                    S3VolumeNameInfo firstFutureInfo = null;

                    Set<String> futureS3Keys = new TreeSet<>();
                    for (SnapshotVolumeDefinition snapVlmDfn : snapDfn.getAllSnapshotVolumeDefinitions(peerCtx))
                    {
                        S3VolumeNameInfo futureInfo = new S3VolumeNameInfo(
                            rscName,
                            "",
                            snapVlmDfn.getVolumeNumber().value,
                            backupTime,
                            s3Suffix,
                            snapName
                        );
                        if (firstFutureInfo == null)
                        {
                            firstFutureInfo = futureInfo;
                        }
                        futureS3Keys.add(futureInfo.toString());
                    }

                    if (firstFutureInfo != null)
                    {
                        BackupApi back = getBackupFromVolumeKey(
                            firstFutureInfo,
                            futureS3Keys,
                            linstorBackupsS3Keys,
                            snapDfn
                        );

                        retIdToBackupsApiMap.put(firstFutureInfo.toString(), back);
                        linstorBackupsS3Keys.add(firstFutureInfo.toString());
                    }
                }
            }
        }

        s3keys.removeAll(linstorBackupsS3Keys);
        return new Pair<>(retIdToBackupsApiMap, s3keys);
    }

    private Pair<BackupApi, Set<String>> getBackupFromMetadata(
        AccessContext peerCtx,
        String metadataKey,
        S3MetafileNameInfo info,
        S3Remote remote,
        Set<String> allS3keys
    )
        throws IOException, AccessDeniedException
    {
        BackupMetaDataPojo s3MetaFile = backupHandler.getMetaFile(metadataKey, remote, peerCtx, getLocalMasterKey());

        Map<Integer, List<BackupMetaInfoPojo>> s3MetaVlmMap = s3MetaFile.getBackups();
        Map<Integer, BackupVolumePojo> retVlmPojoMap = new TreeMap<>(); // vlmNr, vlmPojo

        Set<String> associatedKeys = new TreeSet<>();
        boolean restorable = true;

        for (Entry<Integer, List<BackupMetaInfoPojo>> entry : s3MetaVlmMap.entrySet())
        {
            Integer s3MetaVlmNr = entry.getKey();
            List<BackupMetaInfoPojo> s3BackVlmInfoList = entry.getValue();
            for (BackupMetaInfoPojo s3BackVlmInfo : s3BackVlmInfoList)
            {
                if (!allS3keys.contains(s3BackVlmInfo.getName()))
                {
                    /*
                     * The metafile is referring to a data-file that is not known in the given bucket
                     */
                    restorable = false;
                }
                else
                {
                    try
                    {
                        S3VolumeNameInfo volInfo = new S3VolumeNameInfo(s3BackVlmInfo.getName());
                        if (s3MetaVlmNr == volInfo.vlmNr)
                        {
                            long vlmFinishedTime = s3BackVlmInfo.getFinishedTimestamp();
                            BackupVolumePojo retVlmPojo = new BackupVolumePojo(
                                s3MetaVlmNr,
                                S3Consts.DATE_FORMAT.format(new Date(vlmFinishedTime)),
                                vlmFinishedTime,
                                new BackupVlmS3Pojo(s3BackVlmInfo.getName())
                            );
                            retVlmPojoMap.put(s3MetaVlmNr, retVlmPojo);
                            associatedKeys.add(s3BackVlmInfo.getName());
                        }
                        else
                        {
                            // meta-file vlmNr index corruption
                            restorable = false;
                        }
                    }
                    catch (ParseException ignored)
                    {
                        // meta-file corrupt
                        // s3Key does not match backup name pattern
                        restorable = false;
                    }
                }
            }
        }

        // get rid of ".meta"
        String id = metadataKey.substring(0, metadataKey.length() - 5);
        String basedOn = s3MetaFile.getBasedOn();

        return new Pair<>(
            new BackupPojo(
                id,
                info.rscName,
                info.snapName,
                S3Consts.DATE_FORMAT.format(new Date(s3MetaFile.getStartTimestamp())),
                s3MetaFile.getStartTimestamp(),
                S3Consts.DATE_FORMAT.format(new Date(s3MetaFile.getFinishTimestamp())),
                s3MetaFile.getFinishTimestamp(),
                s3MetaFile.getNodeName(),
                false,
                true,
                restorable,
                retVlmPojoMap,
                basedOn,
                new BackupS3Pojo(metadataKey)
            ),
            associatedKeys
        );
    }

    /**
     * Unlike {@link CtrlApiDataLoader#loadSnapshotDfn(String, String, boolean)} this method does not expect rscDfn to
     * exist when trying to load snapDfn
     *
     * @param rscName
     * @param snapName
     *
     * @return
     */
    private SnapshotDefinition loadSnapDfnIfExists(String rscName, String snapName)
    {
        SnapshotDefinition snapDfn = null;
        try
        {
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, false);
            if (rscDfn != null)
            {
                snapDfn = rscDfn.getSnapshotDfn(
                    peerAccCtx.get(),
                    new SnapshotName(snapName)
                );
            }
        }
        catch (InvalidNameException | AccessDeniedException ignored)
        {
        }
        return snapDfn;
    }

    private BackupApi getBackupFromVolumeKey(
        S3VolumeNameInfo info,
        Set<String> s3keys,
        Set<String> usedKeys,
        SnapshotDefinition snapDfn
    )
    {
        Boolean shipping;
        Boolean success;
        String nodeName = null;
        Map<Integer, BackupVolumePojo> vlms = new TreeMap<>();

        vlms.put(info.vlmNr, new BackupVolumePojo(info.vlmNr, null, null, new BackupVlmS3Pojo(info.toString())));

        try
        {
            AccessContext peerCtx = peerAccCtx.get();

            // get all other matching keys
            // add them to vlms
            // add them to usedKeys
            for (String otherKey : s3keys)
            {
                if (!usedKeys.contains(otherKey) && !otherKey.equals(info.toString()))
                {
                    try
                    {
                        S3VolumeNameInfo otherInfo = new S3VolumeNameInfo(otherKey);
                        if (otherInfo.rscName.equals(info.rscName) && otherInfo.backupId.equals(info.backupId))
                        {
                            vlms.put(
                                otherInfo.vlmNr,
                                new BackupVolumePojo(
                                    otherInfo.vlmNr,
                                    null,
                                    null,
                                    new BackupVlmS3Pojo(otherInfo.toString())
                                )
                            );
                            usedKeys.add(otherKey);
                        }
                    }
                    catch (ParseException ignored)
                    {
                        // Not a volume file
                    }

                }
            }

            // Determine backup status based on snapshot definition
            if (snapDfn != null && snapDfn.getFlags().isSet(peerCtx, SnapshotDefinition.Flags.BACKUP))
            {
                String ts = snapDfn.getProps(peerCtx)
                    .getProp(InternalApiConsts.KEY_BACKUP_START_TIMESTAMP, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                if (ts == null || ts.isEmpty())
                {
                    throw new ImplementationError(
                        "Snapshot " + snapDfn.getName().displayValue +
                            " has the BACKUP-flag set, but does not have a required internal property set."
                    );
                }
                boolean isShipping = snapDfn.getFlags().isSet(peerCtx, SnapshotDefinition.Flags.SHIPPING);
                boolean isShipped = snapDfn.getFlags().isSet(peerCtx, SnapshotDefinition.Flags.SHIPPED);
                if (isShipping || isShipped)
                {
                    for (Snapshot snap : snapDfn.getAllSnapshots(peerCtx))
                    {
                        if (snap.getFlags().isSet(peerCtx, Snapshot.Flags.BACKUP_SOURCE))
                        {
                            nodeName = snap.getNodeName().displayValue;
                        }
                    }
                    if (isShipping)
                    {
                        shipping = true;
                        success = null;
                    }
                    else // if (isShipped)
                    {
                        shipping = false;
                        success = true;
                    }
                }
                else
                {
                    shipping = false;
                    success = false;
                }
            }
            else
            {
                shipping = null;
                success = null;
            }
        }
        catch (AccessDeniedException exc)
        {
            // no access to snapDfn
            shipping = null;
            success = null;
        }

        String id = new S3MetafileNameInfo(info.rscName, info.backupTime, info.s3Suffix, info.snapName)
            .toFullBackupId();

        return new BackupPojo(
            id,
            info.rscName,
            info.snapName,
            S3Consts.DATE_FORMAT.format(info.backupTime),
            info.backupTime.getTime(),
            null,
            null, // TODO: should not be null if success == true
            nodeName,
            shipping,
            success,
            false,
            vlms,
            null,
            null
        );
    }

    public Flux<ApiCallRc> backupAbort(String rscNameRef, boolean restore, boolean create)
    {
        return scopeRunner.fluxInTransactionalScope(
            "abort backup",
            lockGuardFactory.create().read(LockObj.NODES_MAP).write(LockObj.RSC_DFN_MAP).buildDeferred(),
            () -> backupAbortInTransaction(rscNameRef, restore, create)
        );
    }

    private Flux<ApiCallRc> backupAbortInTransaction(String rscNameRef, boolean restorePrm, boolean createPrm)
        throws AccessDeniedException, DatabaseException
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
        Set<SnapshotDefinition> snapDfns = getInProgressBackups(rscDfn);
        if (snapDfns.isEmpty())
        {
            return Flux.empty();
        }

        boolean restore = restorePrm;
        boolean create = createPrm;
        if (!restore && !create)
        {
            if (backupInfoMgr.restoreContainsRscDfn(rscDfn))
            {
                restore = true;
                if (snapDfns.size() > 1)
                {
                    create = true;
                }
            }
            else
            {
                create = true;
            }
        }

        Set<Snapshot> abortRestoreSnaps = backupInfoMgr.abortRestoreGetEntries(rscNameRef);
        Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateStlts = Flux.empty();
        List<SnapshotDefinition> snapDfnsToUpdate = new ArrayList<>();
        for (SnapshotDefinition snapDfn : snapDfns)
        {
            Collection<Snapshot> snaps = snapDfn.getAllSnapshots(peerAccCtx.get());
            boolean abort = false;
            for (Snapshot snap : snaps)
            {
                if (
                    abortRestoreSnaps != null && abortRestoreSnaps.contains(snap) && restore ||
                        snap.getFlags().isSet(peerAccCtx.get(), Snapshot.Flags.BACKUP_SOURCE) && create
                )
                {
                    abort = true;
                }
            }
            if (abort)
            {
                snapDfn.getFlags().disableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
                snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING_ABORT);
                snapDfnsToUpdate.add(snapDfn);
            }
        }

        ctrlTransactionHelper.commit();
        for (SnapshotDefinition snapDfn : snapDfnsToUpdate)
        {
            updateStlts = updateStlts.concatWith(
                ctrlSatelliteUpdateCaller.updateSatellites(snapDfn, CtrlSatelliteUpdateCaller.notConnectedWarn())
            );
        }
        ApiCallRcImpl success = new ApiCallRcImpl();
        success.addEntry(
            "Successfully aborted all " +
                ((create && restore) ?
                    "in-progress backup-shipments and restores" :
                    (create ? "in-progress backup-shipments" : "in-progress backup-restores")) +
                " of resource " + rscNameRef,
            ApiConsts.MASK_SUCCESS
        );
        return updateStlts.transform(
            responses -> CtrlResponseUtils.combineResponses(
                responses,
                LinstorParsingUtils.asRscName(rscNameRef),
                "Abort backups of {1} on {0} started"
            )
        ).concatWith(Flux.just(success));
    }

    private Set<SnapshotDefinition> getInProgressBackups(ResourceDefinition rscDfn) throws AccessDeniedException
    {
        Set<SnapshotDefinition> snapDfns = new HashSet<>();
        for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(peerAccCtx.get()))
        {
            if (
                snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING) &&
                    snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.BACKUP)
            )
            {
                snapDfns.add(snapDfn);
            }
        }
        return snapDfns;
    }

    public Flux<BackupInfoPojo> backupInfo(
        String srcRscName,
        String srcSnapName,
        String backupId,
        Map<String, String> storPoolMapRef,
        String nodeName,
        String remoteName
    )
    {
        Set<NodeName> nodes = Collections.emptySet();
        if (nodeName != null && !nodeName.isEmpty())
        {
            nodes = Collections.singleton(LinstorParsingUtils.asNodeName(nodeName));
        }

        return freeCapacityFetcher
            .fetchThinFreeCapacities(nodes)
            .flatMapMany(
                ignored -> scopeRunner.fluxInTransactionalScope(
                    "restore backup", lockGuardFactory.buildDeferred(
                        LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP
                    ),
                    () -> backupInfoInTransaction(
                        srcRscName,
                        srcSnapName,
                        backupId,
                        storPoolMapRef,
                        nodeName,
                        remoteName
                    )
                )
            );
    }

    private Flux<BackupInfoPojo> backupInfoInTransaction(
        String srcRscName,
        String srcSnapName,
        String backupId,
        Map<String, String> renameMap,
        String nodeName,
        String remoteName
    ) throws AccessDeniedException, InvalidNameException
    {
        S3Remote remote = getS3Remote(remoteName);
        S3MetafileNameInfo metaFile = null;
        byte[] masterKey = getLocalMasterKey();

        List<S3ObjectSummary> objects;
        Set<String> s3keys;

        if (backupId != null && !backupId.isEmpty())
        {
            String metaName = backupId;
            if (!metaName.endsWith(META_SUFFIX))
            {
                metaName = backupId + META_SUFFIX;
            }

            try
            {
                metaFile = new S3MetafileNameInfo(metaName);
                objects = backupHandler.listObjects(metaFile.rscName, remote, peerAccCtx.get(), masterKey);
                s3keys = objects.stream().map(S3ObjectSummary::getKey).collect(Collectors.toCollection(TreeSet::new));
            }
            catch (ParseException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                        "The target backup " + metaName +
                            " is invalid since it does not match the pattern of " +
                            "'<rscName>_back_YYYYMMDD_HHMMSS[optional-backup-s3-suffix][^snapshot-name][.meta]' " +
                            "(e.g. my-rsc_back_20210824_072543)." +
                            "Please provide a valid target backup, or provide only the source resource name " +
                            "to restore to the latest backup of that resource."
                    )
                );
            }
        }
        else
        {
            // No backup was explicitly selected, use the latest available for the source resource.
            objects = backupHandler.listObjects(srcRscName, remote, peerAccCtx.get(), masterKey);
            s3keys = objects.stream().map(S3ObjectSummary::getKey).collect(Collectors.toCollection(TreeSet::new));
            metaFile = getLatestBackup(s3keys, srcSnapName);
        }

        if (metaFile == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_BACKUP | ApiConsts.MASK_BACKUP,
                    "Could not find a matching backup for resource '" + srcRscName + "', snapshot '" + srcSnapName +
                        "' and id '" + backupId + "' in remote '" + remoteName + "'"
                )
            );
        }

        if (!s3keys.contains(metaFile.toString()))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN | ApiConsts.MASK_BACKUP,
                    "Could not find the needed meta-file with the name '" + metaFile + "' in remote '" + remoteName +
                        "'"
                )
            );
        }

        String fullBackup = null;
        String latestBackup = metaFile.toFullBackupId();
        String currentMetaName = metaFile.toString();

        LinkedList<BackupMetaDataPojo> data = new LinkedList<>();
        try
        {
            do
            {
                String toCheck = currentMetaName;
                BackupMetaDataPojo metadata = backupHandler.getMetaFile(toCheck, remote, peerAccCtx.get(), masterKey);
                data.add(metadata);
                currentMetaName = metadata.getBasedOn();
                if (currentMetaName == null)
                {
                    fullBackup = toCheck;
                }
            }
            while (currentMetaName != null);
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                    "Failed to parse meta file " + currentMetaName
                )
            );
        }

        long totalDlSizeKib = 0;
        long totalAllocSizeKib = 0;
        Map<StorPoolApi, List<BackupInfoVlmPojo>> storPoolMap = new HashMap<>();
        List<BackupInfoStorPoolPojo> storpools = new ArrayList<>();

        boolean first = true;
        for (BackupMetaDataPojo meta : data)
        {
            Pair<Long, Long> totalSizes = new Pair<>(0L, 0L); // dlSize, allocSize
            fillBackupInfo(first, storPoolMap, objects, meta, meta.getLayerData(), totalSizes);
            first = false;
            totalDlSizeKib += totalSizes.objA;
            totalAllocSizeKib += totalSizes.objB;
        }
        Map<String, Long> remainingFreeSpace = new HashMap<>();

        if (nodeName != null)
        {
            remainingFreeSpace = getRemainingSize(storPoolMap, renameMap, nodeName);
        }
        for (Entry<StorPoolApi, List<BackupInfoVlmPojo>> entry : storPoolMap.entrySet())
        {
            String targetStorPool = renameMap.get(entry.getKey().getStorPoolName());
            if (targetStorPool == null)
            {
                targetStorPool = entry.getKey().getStorPoolName();
            }
            storpools.add(
                new BackupInfoStorPoolPojo(
                    entry.getKey().getStorPoolName(),
                    entry.getKey().getDeviceProviderKind(),
                    targetStorPool,
                    remainingFreeSpace.get(targetStorPool.toUpperCase()),
                    entry.getValue()
                )
            );
        }

        BackupInfoPojo backupInfo = new BackupInfoPojo(
            metaFile.rscName,
            metaFile.snapName,
            fullBackup,
            latestBackup,
            data.size(),
            totalDlSizeKib,
            totalAllocSizeKib,
            storpools
        );

        return Flux.just(backupInfo);
    }

    private Map<String, Long> getRemainingSize(
        Map<StorPoolApi, List<BackupInfoVlmPojo>> storPoolMap,
        Map<String, String> renameMap,
        String nodeName
    ) throws AccessDeniedException
    {
        Map<String, Long> remainingFreeSpace = new HashMap<>();
        for (Entry<StorPoolApi, List<BackupInfoVlmPojo>> entry : storPoolMap.entrySet())
        {
            String targetStorPool = renameMap.get(entry.getKey().getStorPoolName());
            if (targetStorPool == null)
            {
                targetStorPool = entry.getKey().getStorPoolName();
            }
            long poolAllocSize = 0;
            long poolDlSize = 0;
            StorPool sp = ctrlApiDataLoader.loadStorPool(targetStorPool, nodeName, true);
            Long freeSpace = remainingFreeSpace.get(sp.getName().value);
            if (freeSpace == null)
            {
                freeSpace = sp.getFreeSpaceTracker().getFreeCapacityLastUpdated(sysCtx).orElse(null);
            }
            for (BackupInfoVlmPojo vlm : entry.getValue())
            {
                poolAllocSize += vlm.getAllocSizeKib() != null ? vlm.getAllocSizeKib() : 0;
                poolDlSize += vlm.getDlSizeKib() != null ? vlm.getDlSizeKib() : 0;
            }
            remainingFreeSpace.put(
                sp.getName().value,
                freeSpace != null ? freeSpace - poolAllocSize - poolDlSize : null
            );
        }
        return remainingFreeSpace;
    }

    private void fillBackupInfo(
        boolean first,
        Map<StorPoolApi, List<BackupInfoVlmPojo>> storPoolMap,
        List<S3ObjectSummary> objects,
        BackupMetaDataPojo meta,
        RscLayerDataApi layerData,
        Pair<Long, Long> totalSizes
    )
    {
        for (RscLayerDataApi child : layerData.getChildren())
        {
            fillBackupInfo(first, storPoolMap, objects, meta, child, totalSizes);
        }
        if (layerData.getLayerKind().equals(DeviceLayerKind.STORAGE))
        {
            for (VlmLayerDataApi volume : layerData.getVolumeList())
            {
                if (!storPoolMap.containsKey(volume.getStorPoolApi()))
                {
                    storPoolMap.put(volume.getStorPoolApi(), new ArrayList<>());
                }
                String vlmName = "";
                Long allocSizeKib = null;
                Long useSizeKib = null;
                Long dlSizeKib = null;
                for (BackupMetaInfoPojo backup : meta.getBackups().get(volume.getVlmNr()))
                {
                    try
                    {
                        S3VolumeNameInfo info = new S3VolumeNameInfo(backup.getName());
                        if (info.layerSuffix.equals(layerData.getRscNameSuffix()))
                        {
                            vlmName = backup.getName();
                            break;
                        }
                    }
                    catch (ParseException exc)
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                                "A backup name in the meta-file did not match with the backup-name-pattern." +
                                    " The meta-file is either corrupted or created by an outdated version of linstor."
                            )
                        );
                    }
                }
                if (first)
                {
                    allocSizeKib = volume.getSnapshotAllocatedSize();
                    totalSizes.objB += allocSizeKib;
                    useSizeKib = volume.getSnapshotUsableSize();
                }
                for (S3ObjectSummary object : objects)
                {
                    if (object.getKey().equals(vlmName))
                    {
                        dlSizeKib = (long) Math.ceil(object.getSize() / 1024.0);
                        totalSizes.objA += dlSizeKib;
                        break;
                    }
                }
                DeviceLayerKind layerType = RscLayerSuffixes.getLayerKindFromLastSuffix(layerData.getRscNameSuffix());
                BackupInfoVlmPojo vlmPojo = new BackupInfoVlmPojo(
                    vlmName,
                    layerType,
                    dlSizeKib,
                    allocSizeKib,
                    useSizeKib
                );
                storPoolMap.get(volume.getStorPoolApi()).add(vlmPojo);
            }
        }
    }

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

                Snapshot snap = snapDfn.getSnapshot(peerCtx, peerProvider.get().getNode().getName());
                snapDfn.getFlags().disableFlags(peerCtx, SnapshotDefinition.Flags.SHIPPING);
                Flux<ApiCallRc> l2lCleanupFlux = Flux.empty();
                if (snap != null && !snap.isDeleted())
                {
                    snap.getProps(peerCtx).removeProp(
                        InternalApiConsts.KEY_BACKUP_SRC_REMOTE,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );
                    snap.getProps(peerCtx).removeProp(
                        InternalApiConsts.KEY_BACKUP_TO_RESTORE,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );
                    snap.getFlags().disableFlags(peerCtx, Snapshot.Flags.BACKUP_TARGET);
                    for (Integer port : portsRef)
                    {
                        if (port != null)
                        {
                            snapshotShippingPortPool.deallocate(port);
                        }
                    }

                    boolean keepGoing;

                    Snapshot nextSnap = backupInfoMgr.getNextBackupToDownload(snap);
                    if (successRef && nextSnap != null)
                    {
                        snapDfn.getFlags().enableFlags(peerCtx, SnapshotDefinition.Flags.SHIPPED);
                        backupInfoMgr.abortRestoreDeleteEntry(rscNameRef, snap);
                        SnapshotDefinition nextSnapDfn = nextSnap.getSnapshotDefinition();
                        nextSnapDfn.getFlags().enableFlags(peerCtx, SnapshotDefinition.Flags.SHIPPING);
                        nextSnap.getFlags().enableFlags(peerCtx, Snapshot.Flags.BACKUP_TARGET);

                        ctrlTransactionHelper.commit();
                        flux = ctrlSatelliteUpdateCaller.updateSatellites(
                            snapDfn,
                            CtrlSatelliteUpdateCaller.notConnectedWarn()
                        ).transform(
                            responses -> CtrlResponseUtils.combineResponses(
                                responses,
                                LinstorParsingUtils.asRscName(rscNameRef),
                                "Finishing receiving of backup ''" + snapNameRef + "'' of {1} on {0}"
                            )
                        )
                            .concatWith(snapshotCrtHandler.postCreateSnapshot(nextSnapDfn));
                        keepGoing = true;
                    }
                    else
                    {
                        if (successRef)
                        {
                            snapDfn.getFlags().enableFlags(peerCtx, SnapshotDefinition.Flags.SHIPPED);
                            backupInfoMgr.abortRestoreDeleteAllEntries(rscNameRef);
                            backupInfoMgr.backupsToDownloadCleanUp(snap);
                            backupInfoMgr.restoreRemoveEntry(snapDfn.getResourceDefinition());
                            // start snap-restore
                            if (snapDfn.getFlags().isSet(peerCtx, SnapshotDefinition.Flags.RESTORE_BACKUP_ON_SUCCESS))
                            {
                                // make sure to not restore it a second time
                                snapDfn.getFlags().disableFlags(
                                    peerCtx,
                                    SnapshotDefinition.Flags.RESTORE_BACKUP_ON_SUCCESS
                                );
                                flux = ctrlSnapRestoreApiCallHandler.restoreSnapshotFromBackup(
                                    Collections.emptyList(),
                                    snapNameRef,
                                    rscNameRef
                                );
                            }
                            else
                            {
                                /*
                                 * no need for a "successfully downloaded" message, as this flux is triggered
                                 * by the satellite who does not care about this kind of ApiCallRc message
                                 */
                                flux = Flux.empty();
                            }
                            keepGoing = false; // we received the last backup
                        }
                        else
                        {
                            List<SnapshotDefinition> snapsToDelete = new ArrayList<>();
                            snapsToDelete.add(snapDfn);
                            backupInfoMgr.abortRestoreDeleteEntry(rscNameRef, snap);
                            backupInfoMgr.restoreRemoveEntry(snapDfn.getResourceDefinition());
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
                                backupInfoMgr.abortRestoreDeleteAllEntries(rscNameRef);
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
                            keepGoing = false; // last backup failed.
                        }
                    }

                    if (!keepGoing)
                    {
                        String remoteName = snap.getProps(peerAccCtx.get()).getProp(
                            InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                            ApiConsts.NAMESPC_BACKUP_SHIPPING
                        );
                        Remote remote = remoteRepo.get(sysCtx, new RemoteName(remoteName, true));
                        if (remote != null && remote instanceof StltRemote)
                        {
                            snap.getProps(peerAccCtx.get()).removeProp(
                                InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                                ApiConsts.NAMESPC_BACKUP_SHIPPING
                            );
                            Flux<ApiCallRc> tmpFlux = ctrlSatelliteUpdateCaller.updateSatellites(
                                snapDfn,
                                CtrlSatelliteUpdateCaller.notConnectedWarn()
                            ).transform(
                                responses -> CtrlResponseUtils.combineResponses(
                                    responses,
                                    LinstorParsingUtils.asRscName(rscNameRef),
                                    "Removing remote property from snapshot '" + snapNameRef + "' of {1} on {0}"
                                )
                            );
                            // cleanupStltRemote will not be executed if flux has an error - this issue is currently
                            // unavoidable.
                            // This will be fixed with the linstor2 issue 19 (Combine Changed* proto messages for atomic
                            // updates)
                            l2lCleanupFlux = tmpFlux
                                .concatWith(cleanupStltRemote((StltRemote) remote));
                        }
                    }
                }
                ctrlTransactionHelper.commit();
                startStltCleanup(peerProvider.get(), rscNameRef, snapNameRef);
                flux = l2lCleanupFlux.concatWith(flux);
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

    public Flux<ApiCallRc> shippingSent(
        String rscNameRef,
        String snapNameRef,
        List<Integer> ignored,
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

    private Flux<ApiCallRc> shippingSentInTransaction(String rscNameRef, String snapNameRef, boolean successRef)
    {
        errorReporter.logInfo(
            "Backup shipping for snapshot %s of resource %s %s", snapNameRef, rscNameRef,
            successRef ? "finished successfully" : "failed"
        );
        SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameRef, snapNameRef, false);
        Flux<ApiCallRc> cleanupFlux = Flux.empty();
        if (snapDfn != null)
        {
            try
            {
                NodeName nodeName = peerProvider.get().getNode().getName();
                backupInfoMgr.abortCreateDeleteEntries(nodeName.displayValue, rscNameRef, snapNameRef);
                if (!successRef && snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING_ABORT))
                {
                    // re-enable shipping-flag to make sure the abort-logic gets triggered later on
                    snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
                    ctrlTransactionHelper.commit();
                    startStltCleanup(peerProvider.get(), rscNameRef, snapNameRef);
                    cleanupFlux = ctrlSnapShipAbortHandler
                        .abortBackupShippingPrivileged(snapDfn.getResourceDefinition());
                }
                else
                {
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
                        snap.getProps(peerAccCtx.get()).removeProp(
                            InternalApiConsts.KEY_BACKUP_NODE_IDS_TO_RESET,
                            ApiConsts.NAMESPC_BACKUP_SHIPPING
                        );
                        snap.getFlags().disableFlags(sysCtx, Snapshot.Flags.BACKUP_SOURCE);
                        Remote remote = remoteRepo.get(sysCtx, new RemoteName(remoteName, true));
                        if (remote != null)
                        {
                            if (successRef)
                            {
                                ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
                                rscDfn.getProps(peerAccCtx.get()).setProp(
                                    InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                                    snapNameRef,
                                    ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" + remoteName
                                );
                            }
                            if (remote instanceof StltRemote)
                            {
                                cleanupFlux = cleanupStltRemote((StltRemote) remote);
                            }
                        }
                        else
                        {
                            throw new ImplementationError("Unknown remote. successRef: " + successRef);
                        }
                    }
                    ctrlTransactionHelper.commit();
                    startStltCleanup(peerProvider.get(), rscNameRef, snapNameRef);
                    Flux<ApiCallRc> flux = ctrlSatelliteUpdateCaller.updateSatellites(
                        snapDfn,
                        CtrlSatelliteUpdateCaller.notConnectedWarn()
                    ).transform(
                        responses -> CtrlResponseUtils.combineResponses(
                            responses,
                            LinstorParsingUtils.asRscName(rscNameRef),
                            "Finishing shipping of backup ''" + snapNameRef + "'' of {1} on {0}"
                        )
                    );
                    // cleanupFlux will not be executed if flux has an error - this issue is currently unavoidable.
                    // This will be fixed with the linstor2 issue 19 (Combine Changed* proto messages for atomic
                    // updates)
                    cleanupFlux = flux.concatWith(cleanupFlux);
                }
            }
            catch (AccessDeniedException | InvalidNameException | InvalidValueException | InvalidKeyException exc)
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

    private void startStltCleanup(Peer peer, String rscNameRef, String snapNameRef)
    {
        // needed to remove snap from the "shipping started" list. This action
        // can't be done by the stlt itself because it might be to fast so that a second shipping is triggered by an
        // unrelated update
        byte[] msg = stltComSerializer.headerlessBuilder().notifyBackupShippingFinished(rscNameRef, snapNameRef)
            .build();
        peer.apiCall(InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_FINISHED, msg);
    }

    private Flux<ApiCallRc> cleanupStltRemote(StltRemote remote)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Cleanup Stlt-Remote",
                lockGuardFactory.create()
                    .write(LockObj.REMOTE_MAP).buildDeferred(),
                () -> cleanupStltRemoteInTransaction(remote)
            );
    }

    private Flux<ApiCallRc> cleanupStltRemoteInTransaction(StltRemote remote)
    {
        Flux<ApiCallRc> flux;
        try
        {
            remote.getFlags().enableFlags(sysCtx, StltRemote.Flags.DELETE);
            ctrlTransactionHelper.commit();
            flux = ctrlSatelliteUpdateCaller.updateSatellites(remote)
                .concatWith(
                    scopeRunner.fluxInTransactionalScope(
                        "Removing temporary satellite remote",
                        lockGuardFactory.create()
                            .write(LockObj.REMOTE_MAP).buildDeferred(),
                        () -> deleteStltRemoteInTransaction(remote.getName().displayValue)
                    )
                );
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        return flux;
    }

    private Flux<ApiCallRc> deleteStltRemoteInTransaction(String remoteNameRef)
    {
        Remote remote;
        try
        {
            remote = remoteRepo.get(sysCtx, new RemoteName(remoteNameRef, true));
            if (!(remote instanceof StltRemote))
            {
                throw new ImplementationError("This method should only be called for satellite remotes");
            }
            remoteRepo.remove(sysCtx, remote.getName());
            remote.delete(sysCtx);

            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException | InvalidNameException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        return Flux.empty();
    }

    private ApiCallRcImpl backupShippingSupported(Resource rsc) throws AccessDeniedException
    {
        Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc, peerAccCtx.get());
        ApiCallRcImpl errors = new ApiCallRcImpl();
        for (StorPool sp : storPools)
        {
            DeviceProviderKind deviceProviderKind = sp.getDeviceProviderKind();
            if (deviceProviderKind.isSnapshotShippingSupported())
            {
                ExtToolsManager extToolsManager = rsc.getNode().getPeer(peerAccCtx.get()).getExtToolsManager();
                errors.addEntry(
                    getErrorRcIfNotSupported(
                        deviceProviderKind,
                        extToolsManager,
                        ExtTools.UTIL_LINUX,
                        "setsid from util_linux",
                        new ExtToolsInfo.Version(2, 24)
                    )
                );
                if (deviceProviderKind.equals(DeviceProviderKind.LVM_THIN))
                {
                    errors.addEntry(
                        getErrorRcIfNotSupported(
                            deviceProviderKind,
                            extToolsManager,
                            ExtTools.THIN_SEND_RECV,
                            "thin_send_recv",
                            new ExtToolsInfo.Version(0, 24)
                        )
                    );
                }
            }
            else
            {
                errors.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_SNAPSHOT_SHIPPING_NOT_SUPPORTED,
                        String.format(
                            "The storage pool kind %s does not support snapshot shipping",
                            deviceProviderKind.name()
                        )
                    )
                );
            }
        }
        return errors;
    }

    private ApiCallRcEntry getErrorRcIfNotSupported(
        DeviceProviderKind deviceProviderKind,
        ExtToolsManager extToolsManagerRef,
        ExtTools extTool,
        String toolDescr,
        ExtToolsInfo.Version version
    )
    {
        ApiCallRcEntry errorRc;
        ExtToolsInfo info = extToolsManagerRef.getExtToolInfo(extTool);
        if (info == null || !info.isSupported())
        {
            errorRc = ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_SNAPSHOT_SHIPPING_NOT_SUPPORTED,
                String.format(
                    "%s based backup shipping requires support for %s",
                    deviceProviderKind.name(),
                    toolDescr
                ),
                true
            );
        }
        else
        if (version != null && !info.hasVersionOrHigher(version))
        {
            errorRc = ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_SNAPSHOT_SHIPPING_NOT_SUPPORTED,
                String.format(
                    "%s based backup shipping requires at least version %s for %s",
                    deviceProviderKind.name(),
                    version.toString(),
                    toolDescr
                ),
                true
            );
        }
        else
        {
            errorRc = null;
        }
        return errorRc;
    }

    private byte[] getLocalMasterKey()
    {
        byte[] masterKey = ctrlSecObj.getCryptKey();
        if (masterKey == null || masterKey.length == 0)
        {
            throw new ApiRcException(
                ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY,
                        "Unable to decrypt the S3 access key and secret key without having a master key"
                    )
                    .setCause("The masterkey was not initialized yet")
                    .setCorrection("Create or enter the master passphrase")
                    .build()
            );
        }
        return masterKey;
    }

    private S3Remote getS3Remote(String remoteName) throws AccessDeniedException, InvalidNameException
    {
        if (remoteName == null || remoteName.isEmpty())
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REMOTE_NAME | ApiConsts.MASK_BACKUP,
                    "No remote name was given. Please provide a valid remote name."
                )
            );
        }
        S3Remote remote = remoteRepo.getS3(peerAccCtx.get(), new RemoteName(remoteName));
        if (remote == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REMOTE_NAME | ApiConsts.MASK_BACKUP,
                    "The remote " + remoteName + " does not exist. Please provide a valid remote or create a new one."
                )
            );
        }
        return remote;
    }

    private static class S3ObjectInfo
    {
        private String s3Key;
        private boolean exists = false;
        private BackupMetaDataPojo metaFile;
        private SnapshotDefinition snapDfn = null;
        private HashSet<S3ObjectInfo> referencedBy = new HashSet<>();
        private HashSet<S3ObjectInfo> references = new HashSet<>();

        private S3ObjectInfo(String s3KeyRef)
        {
            s3Key = s3KeyRef;
        }

        public boolean isMetaFile()
        {
            return metaFile != null;
        }

        @Override
        public String toString()
        {
            return "S3ObjectInfo [s3Key=" + s3Key + "]";
        }
    }

    private static class ToDeleteCollections
    {
        Set<String> s3keys;
        Set<SnapshotDefinition.Key> snapKeys;
        ApiCallRcImpl apiCallRcs;
        Set<String> s3KeysNotFound;

        ToDeleteCollections()
        {
            s3keys = new TreeSet<>();
            snapKeys = new TreeSet<>();
            apiCallRcs = new ApiCallRcImpl();
            s3KeysNotFound = new TreeSet<>();
        }
    }
}
