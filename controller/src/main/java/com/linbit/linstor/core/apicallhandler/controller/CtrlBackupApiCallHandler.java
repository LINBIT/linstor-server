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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    private static final Pattern META_FILE_PATTERN = Pattern
        .compile("^([a-zA-Z0-9_-]{2,48})_(back_[0-9]{8}_[0-9]{6})(:?.*)\\.meta$");
    private static final Pattern BACKUP_VOLUME_PATTERN = Pattern
        .compile("^([a-zA-Z0-9_-]{2,48})(\\..+)?_([0-9]{5})_(back_[0-9]{8}_[0-9]{6})(:?.*)$");

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
        String remoteNameRef,
        String nodeNameRef,
        boolean incremental
    )
        throws AccessDeniedException
    {
        long now = System.currentTimeMillis();
        String snapName = generateNewSnapshotName(new Date(now));
        Flux<ApiCallRc> response = scopeRunner.fluxInTransactionalScope(
            "Backup snapshot",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> backupSnapshot(
                rscNameRef,
                remoteNameRef,
                nodeNameRef,
                snapName,
                now,
                true,
                incremental,
                Collections.singletonMap(ExtTools.ZSTD, null),
                null,
                RemoteType.S3
            ).objA
        );

        return response;
    }

    public static String generateNewSnapshotName(Date date)
    {
        return S3Consts.SNAP_PREFIX + S3Consts.DATE_FORMAT.format(date);
    }

    Pair<Flux<ApiCallRc>, Snapshot> backupSnapshot(
        String rscNameRef,
        String remoteName,
        String nodeName,
        String snapName,
        long nowRef,
        boolean setShippingFlag,
        boolean allowIncremental,
        Map<ExtTools, ExtToolsInfo.Version> requiredExtTools,
        Map<ExtTools, ExtToolsInfo.Version> optionalExtTools,
        RemoteType remoteTypeRef
    )
    {
        try
        {
            ApiCallRcImpl responses = new ApiCallRcImpl();

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

            SnapshotDefinition prevSnapDfn = null;
            String prevSnapName = rscDfn.getProps(peerAccCtx.get()).getProp(
                InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT, ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" + remoteName
            );
            boolean incremental = allowIncremental;
            if (incremental)
            {
                if (prevSnapName == null)
                {
                    errorReporter.logWarning(
                        "Could not create an incremental backup for resource %s as there is no previous " +
                            "full backup. Creating a full backup instead.",
                        rscNameRef
                    );
                    incremental = false;
                }
                else
                {
                    prevSnapDfn = ctrlApiDataLoader.loadSnapshotDfn(
                        rscDfn,
                        new SnapshotName(prevSnapName),
                        false
                    );
                    if (prevSnapDfn == null)
                    {
                        errorReporter.logWarning(
                            "Could not create an incremental backup for resource %s as the previous snapshot " +
                                "%s needed for the incremental backup has already been deleted. Creating a full " +
                                "backup instead.",
                            rscNameRef, prevSnapName
                        );
                        incremental = false;
                    }
                    else
                    {
                        boolean sizeMatches = true;
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
                                sizeMatches = false;
                                break;
                            }
                        }
                        if (!sizeMatches)
                        {
                            prevSnapDfn = null; // force full backup
                            incremental = false;
                            responses.addEntry(
                                ApiCallRcImpl.simpleEntry(
                                    ApiConsts.MASK_WARN,
                                    "Forcing full backup as volume sizes changed since last backup"
                                )
                            );
                        }
                    }
                }
            }

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
                Long.toString(nowRef),
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

            setPropsIncremantaldependendProps(snapName, incremental, prevSnapDfn, createdSnapshot);

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

    void setPropsIncremantaldependendProps(
        String snapName,
        boolean incremental,
        SnapshotDefinition prevSnapDfn,
        Snapshot snap
    )
        throws InvalidValueException, AccessDeniedException, DatabaseException
    {
        SnapshotDefinition snapDfn = snap.getSnapshotDefinition();
        if (!incremental)
        {
            snapDfn.getProps(peerAccCtx.get())
                .setProp(
                    InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP,
                    snapName,
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
                /*
                 * DO NOT use s3Obj.metaFile.getStartTimestamp()
                 *
                 * DATE_FORMAT.parse might return a timestamp within a different timezome than the timestamp of the
                 * metafile.
                 * We need to make sure to use the same timezone. Therefore another parse of metaFile.getStartTime
                 * (string based)
                 * is needed.
                 */
                String timeStr;
                {
                    Matcher mtc = META_FILE_PATTERN.matcher(s3Obj.s3Key);
                    if (!mtc.find())
                    {
                        throw new ImplementationError("Unexpected meta filename");
                    }
                    timeStr = mtc.group(2).substring(S3Consts.SNAP_PREFIX_LEN);
                }
                long startTimestamp;
                try
                {
                    startTimestamp = S3Consts.DATE_FORMAT.parse(timeStr).getTime();
                }
                catch (ParseException exc)
                {
                    throw new ImplementationError("Unexpected date format: " + timeStr);
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
        // add all backups to the list that have useable metadata-files
        for (String s3Key : allS3KeyRef)
        {
            Matcher metaFileMatcher = META_FILE_PATTERN.matcher(s3Key);
            if (metaFileMatcher.matches())
            {
                // s3 key at least has linstor's meta file format
                // we still need to check if it is valid though
                try
                {
                    // throws parse exception if not linstor json
                    BackupMetaDataPojo s3MetaFile = backupHandler.getMetaFile(
                        s3Key,
                        s3RemoteRef,
                        peerAccCtx.get(),
                        getLocalMasterKey()
                    );
                    S3ObjectInfo metaInfo = ret.get(s3Key);
                    if (metaInfo == null)
                    {
                        metaInfo = new S3ObjectInfo(s3Key);
                        ret.put(s3Key, metaInfo);
                    }
                    metaInfo.exists = true;
                    metaInfo.metaFile = s3MetaFile;
                    for (List<BackupMetaInfoPojo> backupInfoPojoList : s3MetaFile.getBackups().values())
                    {
                        for (BackupMetaInfoPojo backupInfoPojo : backupInfoPojoList)
                        {
                            String childS3Key = backupInfoPojo.getName();
                            S3ObjectInfo childS3Obj = ret.get(childS3Key);
                            if (childS3Obj == null)
                            {
                                childS3Obj = new S3ObjectInfo(childS3Key);
                                ret.put(childS3Key, childS3Obj);
                            }
                            childS3Obj.referencedBy.add(metaInfo);
                            metaInfo.references.add(childS3Obj);
                        }
                    }
                    String rscName = metaFileMatcher.group(1);
                    String snapName = metaFileMatcher.group(2);
                    SnapshotDefinition snapDfn = loadSnapDfnIfExists(rscName, snapName);
                    if (snapDfn != null)
                    {
                        if (snapDfn.getUuid().toString().equals(s3MetaFile.getSnapDfnUuid()))
                        {
                            metaInfo.snapDfn = snapDfn;
                        }
                        else
                        {
                            apiCallRc.addEntry(
                                "Not marking SnapshotDefinition " + rscName + " / " +
                                    snapName + " for deletion as the UUID does not match with the backup",
                                ApiConsts.WARN_NOT_FOUND
                            );
                        }
                    }

                    String basedOnS3Key = s3MetaFile.getBasedOn();
                    if (basedOnS3Key != null)
                    {
                        S3ObjectInfo basedOnS3MetaInfo = ret.get(basedOnS3Key);
                        if (basedOnS3MetaInfo == null)
                        {
                            basedOnS3MetaInfo = new S3ObjectInfo(basedOnS3Key);
                            ret.put(basedOnS3Key, basedOnS3MetaInfo);
                        }
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
            }
            else
            {
                Matcher s3BackupKeyMatcher = BACKUP_VOLUME_PATTERN.matcher(s3Key);
                if (s3BackupKeyMatcher.matches())
                {
                    S3ObjectInfo s3DataInfo = ret.get(s3Key);
                    if (s3DataInfo == null)
                    {
                        s3DataInfo = new S3ObjectInfo(s3Key);
                        ret.put(s3Key, s3DataInfo);
                    }
                    s3DataInfo.exists = true;
                    String rscName = s3BackupKeyMatcher.group(1);
                    String snapName = s3BackupKeyMatcher.group(4);
                    s3DataInfo.snapDfn = loadSnapDfnIfExists(rscName, snapName);
                }
            }
        }
        return ret;
    }

    public Flux<ApiCallRc> restoreBackup(
        String srcRscName,
        Map<String, String> storPoolMapRef,
        String nodeName,
        String targetRscName,
        String remoteName,
        String passphrase,
        String lastBackup,
        boolean downloadOnly
    )
    {
        return freeCapacityFetcher
            .fetchThinFreeCapacities(
                Collections.singleton(LinstorParsingUtils.asNodeName(nodeName))
            ).flatMapMany(
                thinFreeCapacities -> scopeRunner.fluxInTransactionalScope(
                    "restore backup", lockGuardFactory.buildDeferred(
                        LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP
                    ),
                    () -> restoreBackupInTransaction(
                        srcRscName,
                        storPoolMapRef,
                        nodeName,
                        thinFreeCapacities,
                        targetRscName,
                        remoteName,
                        passphrase,
                        lastBackup,
                        downloadOnly
                    )
                )
            );
    }

    private Flux<ApiCallRc> restoreBackupInTransaction(
        String srcRscName,
        Map<String, String> storPoolMap,
        String nodeName,
        Map<StorPool.Key, Long> thinFreeCapacities,
        String targetRscName,
        String remoteName,
        String passphrase,
        String lastBackup,
        boolean downloadOnly
    ) throws AccessDeniedException, InvalidNameException
    {

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
        Date targetTime = null;
        String shortTargetName = null;
        String adjLastBackup = lastBackup;
        String curSrcRscName = srcRscName;
        if (adjLastBackup != null && !adjLastBackup.isEmpty())
        {
            if (!adjLastBackup.endsWith(META_SUFFIX))
            {
                adjLastBackup = adjLastBackup + META_SUFFIX;
            }
            Matcher targetMatcher = META_FILE_PATTERN.matcher(adjLastBackup);
            if (targetMatcher.matches())
            {
                curSrcRscName = targetMatcher.group(1);
                String snapName = targetMatcher.group(2);
                shortTargetName = targetMatcher.group(1) + "_" + snapName;
                try
                {
                    targetTime = S3Consts.DATE_FORMAT.parse(snapName.substring(S3Consts.SNAP_PREFIX_LEN));
                }
                catch (ParseException exc)
                {
                    errorReporter.reportError(exc, peerAccCtx.get(), null, "used s3 key: " + adjLastBackup);
                }
            }
            else
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                        "The target backup " + adjLastBackup +
                            " is invalid since it does not match the pattern of " +
                            "'<rscName>_back_YYYYMMDD_HHMMSS<optional-backup-s3-suffix> " +
                            "(e.g. my-rsc_back_20210824_072543)" +
                            META_FILE_PATTERN + "'. " +
                            "Please provide a valid target backup, or provide only the source resource name to " +
                            "restore to the latest backup of that resource."
                    )
                );
            }
        }
        S3Remote remote = getS3Remote(remoteName);
        byte[] targetMasterKey = getLocalMasterKey();
        // 1. list srcRscName*
        List<S3ObjectSummary> objects = backupHandler
            .listObjects(curSrcRscName, remote, peerAccCtx.get(), targetMasterKey);
        Set<String> s3keys = objects.stream()
            .map(S3ObjectSummary::getKey)
            .collect(Collectors.toCollection(TreeSet::new));
        if (targetTime != null && !s3keys.contains(adjLastBackup))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                    "The target backup " + adjLastBackup +
                        " is invalid since it does not exist in the given remote " + remoteName + ". " +
                        "Please provide a valid target backup, or provide only the source resource name to " +
                            "restore to the latest backup of that resource."
                )
            );
        }
        // 2. find meta-file
        String metaName = "";
        Date latestBackTs;
        try
        {
            metaName = getLatestMetaFile(s3keys, targetTime);
            if (metaName == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN | ApiConsts.MASK_BACKUP,
                        "Could not find backups with the given resource name '" + curSrcRscName + "' in remote '" +
                            remoteName + "'"
                    )
                );
            }
            Matcher mtc = META_FILE_PATTERN.matcher(metaName);
            if (mtc.matches())
            {
                latestBackTs = S3Consts.DATE_FORMAT.parse(mtc.group(2).substring(S3Consts.SNAP_PREFIX_LEN));
            }
            else
            {
                throw new ImplementationError(metaName + " did not match the expected pattern.");
            }
        }
        catch (ParseException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP, "Tried to parse s3 key " + metaName
                ),
                exc
            );
        }

        if (backupInfoMgr.restoreContainsMetaFile(metaName))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_IN_USE | ApiConsts.MASK_BACKUP,
                    "The meta-file " + metaName + " is currently being used in a restore."
                )
            );
        }
        // 3. get meta-file
        try
        {
            /*
             * By default (or user-choice) the current metadata will be the latest version of the backup, including all
             * incremental backups.
             * In order to restore that, we need to start with the full backup, continue with the first, second ,... and
             * finally the last incremental backup
             */
            Snapshot nextBackup = null;
            boolean resetData = true; // reset data based on the final snapshot
            String lastMetaName = metaName;
            Map<StorPoolApi, List<BackupInfoVlmPojo>> storPoolInfo = new HashMap<>();
            List<Pair<String, BackupMetaDataPojo>> data = new ArrayList<>();
            do
            {
                BackupMetaDataPojo metadata = backupHandler
                    .getMetaFile(metaName, remote, peerAccCtx.get(), targetMasterKey);
                data.add(new Pair<>(metaName, metadata));
                metaName = metadata.getBasedOn();
            }
            while (metaName != null);

            // check if given storpools have enough space remaining for the restore
            boolean first = true;
            for (Pair<String, BackupMetaDataPojo> meta : data)
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

            ApiCallRcImpl responses = new ApiCallRcImpl();
            for (Pair<String, BackupMetaDataPojo> metadata : data)
            {
                Snapshot snap = createSnapshotByS3Meta(
                    curSrcRscName,
                    storPoolMap,
                    node,
                    targetRscName,
                    passphrase,
                    shortTargetName,
                    remote,
                    s3keys,
                    latestBackTs,
                    metadata.objA,
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
                    curSrcRscName + "_" + snap.getSnapshotName().displayValue,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                );
                if (nextBackup != null)
                {
                    backupInfoMgr.backupsToDownloadAddEntry(snap, nextBackup);
                }
                nextBackup = snap;
            }
            /*
             * we went through the snapshots from the newest to the oldest.
             * That means "nextBackup" now is the base-/full-backup which we want to start restore with
             */
            nextBackup.getFlags().enableFlags(peerAccCtx.get(), Snapshot.Flags.BACKUP_TARGET);
            if (!backupInfoMgr.restoreAddEntry(nextBackup.getResourceDefinition(), lastMetaName))
            {
                throw new ImplementationError(
                    "Tried to overwrite existing backup-info-mgr entry for rscDfn " + targetRscName
                );
            }
            ctrlTransactionHelper.commit();
            responses.addEntry(
                "Restoring backup of resource " + curSrcRscName + " from remote " + remoteName +
                    " into resource " + targetRscName + " in progress.",
                ApiConsts.MASK_INFO
            );
            SnapshotDefinition nextBackSnapDfn = nextBackup.getSnapshotDefinition();
            return snapshotCrtHandler.postCreateSnapshot(nextBackSnapDfn)
                .concatWith(Flux.just(responses))
                .onErrorResume(error -> cleanupAfterFailedRestore(error, nextBackSnapDfn));
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                    "Failed to parse meta file " + metaName
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

    private String getLatestMetaFile(Set<String> s3keys, Date targetTime) throws ParseException
    {
        String metaName = null;
        Date latestBackTs = null;
        for (String s3key : s3keys)
        {
            Matcher mtc = META_FILE_PATTERN.matcher(s3key);
            if (mtc.matches())
            {
                // remove "back_" prefix
                String ts = mtc.group(2).substring(S3Consts.SNAP_PREFIX_LEN);
                Date curTs = S3Consts.DATE_FORMAT.parse(ts);
                if (targetTime != null)
                {
                    if (
                        (latestBackTs == null || latestBackTs.before(curTs)) &&
                            (targetTime.after(curTs) || targetTime.equals(curTs))
                    )
                    {
                        metaName = mtc.group();
                        latestBackTs = curTs;
                    }
                }
                else
                {
                    if (latestBackTs == null || latestBackTs.before(curTs))
                    {
                        metaName = mtc.group();
                        latestBackTs = curTs;
                    }
                }
            }
        }
        return metaName;
    }

    private Snapshot createSnapshotByS3Meta(
        String srcRscName,
        Map<String, String> storPoolMap,
        Node node,
        String targetRscName,
        String passphrase,
        String shortTargetName,
        S3Remote remote,
        Set<String> s3keys,
        Date latestBackTs,
        String metaName,
        BackupMetaDataPojo metadata,
        ApiCallRcImpl responses,
        boolean resetData,
        boolean downloadOnly
    )
        throws AccessDeniedException, ImplementationError, DatabaseException, InvalidValueException
    {
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
        // 5. create layerPayload
        RscLayerDataApi layers = metadata.getLayerData();
        // 6. do luks-stuff if needed
        LuksLayerMetaPojo luksInfo = metadata.getLuksInfo();
        byte[] srcMasterKey = null;
        if (luksInfo != null)
        {
            if (passphrase == null || passphrase.isEmpty())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY | ApiConsts.MASK_BACKUP,
                        "The resource " + srcRscName +
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
                        "Some of the needed properties were not set in the metadata-file " + metaName +
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
        // 8. create rscDfn
        Matcher mtc = META_FILE_PATTERN.matcher(metaName);
        if (!mtc.find())
        {
            throw new ImplementationError("not a meta file: " + metaName);
        }
        SnapshotName snapName = LinstorParsingUtils.asSnapshotName(mtc.group(2));
        ResourceDefinition rscDfn = getRscDfnForBackupRestore(targetRscName, snapName, metadata, false, resetData);
        // 9. create snapDfn
        SnapshotDefinition snapDfn = getSnapDfnForBackupRestore(metadata, snapName, rscDfn, responses, downloadOnly);
        // 10. create vlmDfn(s)
        // 11. create snapVlmDfn(s)
        Map<Integer, SnapshotVolumeDefinition> snapVlmDfns = new TreeMap<>();
        createSnapVlmDfnForBackupRestore(
            targetRscName,
            metadata,
            rscDfn,
            snapDfn,
            snapVlmDfns,
            resetData
        );

        Snapshot snap = createSnapshotAndVolumesForBackupRestore(
            metadata,
            layers,
            node,
            snapDfn,
            snapVlmDfns,
            storPoolMap,
            remote,
            shortTargetName
        );
        // 13. create snapshotVlm(s)

        // LUKS
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
                    data.stltRemote,
                    null
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
        Remote remote,
        String shortTargetName
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
    public Pair<Map<String, BackupApi>, Set<String>> listBackups(String rscNameRef, String remoteNameRef)
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
        final String fullBackupKey = "";

        Set<String> linstorBackupsS3Keys = new TreeSet<>();

        // add all backups to the list that have useable metadata-files
        for (String s3key : s3keys)
        {
            Matcher metaFileMatcher = META_FILE_PATTERN.matcher(s3key);
            if (metaFileMatcher.matches())
            {
                // s3 key at least has linstor's meta file format
                // we still need to check if it is valid though
                try
                {
                    // throws parse exception if not linstor json
                    BackupMetaDataPojo s3MetaFile = backupHandler.getMetaFile(
                        s3key,
                        remote,
                        peerCtx,
                        getLocalMasterKey()
                    );

                    Map<Integer, List<BackupMetaInfoPojo>> s3MetaVlmMap = s3MetaFile.getBackups();
                    Map<Integer, BackupVolumePojo> retVlmPojoMap = new TreeMap<>(); // vlmNr, vlmPojo
                    boolean restorable = true;

                    for (Entry<Integer, List<BackupMetaInfoPojo>> entry : s3MetaVlmMap.entrySet())
                    {
                        Integer s3MetaVlmNr = entry.getKey();
                        List<BackupMetaInfoPojo> s3BackVlmInfoList = entry.getValue();
                        for (BackupMetaInfoPojo s3BackVlmInfo : s3BackVlmInfoList)
                        {
                            if (!s3keys.contains(s3BackVlmInfo.getName()))
                            {
                                /*
                                 * The metafile is referring to a data-file that is not known in the given bucket
                                 */
                                restorable = false;
                            }
                            else
                            {

                                Matcher s3BackupKeyMatcher = BACKUP_VOLUME_PATTERN.matcher(s3BackVlmInfo.getName());
                                if (s3BackupKeyMatcher.matches())
                                {
                                    Integer s3VlmNrFromBackupName = Integer.parseInt(s3BackupKeyMatcher.group(3));
                                    if (s3MetaVlmNr.equals(s3VlmNrFromBackupName))
                                    {
                                        long vlmFinishedTime = s3BackVlmInfo.getFinishedTimestamp();
                                        BackupVolumePojo retVlmPojo = new BackupVolumePojo(
                                            s3MetaVlmNr,
                                            S3Consts.DATE_FORMAT.format(new Date(vlmFinishedTime)),
                                            vlmFinishedTime,
                                            new BackupVlmS3Pojo(s3BackVlmInfo.getName())
                                        );
                                        retVlmPojoMap.put(s3MetaVlmNr, retVlmPojo);
                                        linstorBackupsS3Keys.add(s3BackVlmInfo.getName());
                                    }
                                    else
                                    {
                                        // meta-file vlmNr index corruption
                                        restorable = false;
                                    }
                                }
                                else
                                {
                                    // meta-file corrupt
                                    // s3Key does not match backup name pattern
                                    restorable = false;
                                }
                            }
                        }
                    }

                    // get rid of ".meta"
                    String id = s3key.substring(0, s3key.length() - 5);
                    String basedOn = s3MetaFile.getBasedOn();
                    BackupApi back = new BackupPojo(
                        id,
                        metaFileMatcher.group(1),
                        metaFileMatcher.group(2),
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
                        new BackupS3Pojo(s3key)
                    );
                    retIdToBackupsApiMap.put(id, back);
                    if (basedOn == null)
                    {
                        basedOn = fullBackupKey;
                    }
                    List<BackupApi> usedByList = idToUsedByBackupApiMap.get(basedOn);
                    if (usedByList == null)
                    {
                        usedByList = new ArrayList<>();
                        idToUsedByBackupApiMap.put(basedOn, usedByList);
                    }
                    usedByList.add(back);
                    linstorBackupsS3Keys.add(s3key);
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
                Matcher mtc = BACKUP_VOLUME_PATTERN.matcher(s3key);
                if (mtc.matches())
                {
                    String rscName = mtc.group(1);
                    String snapName = mtc.group(4);

                    SnapshotDefinition snapDfn = loadSnapDfnIfExists(rscName, snapName);

                    BackupApi back = fillBackupListPojo(
                        s3key,
                        rscName,
                        snapName,
                        mtc,
                        BACKUP_VOLUME_PATTERN,
                        s3keys,
                        linstorBackupsS3Keys,
                        snapDfn
                    );
                    if (back != null)
                    {
                        retIdToBackupsApiMap.put(s3key, back);
                        linstorBackupsS3Keys.add(s3key);
                    }
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
                    Set<String> futureS3Keys = new TreeSet<>();
                    for (SnapshotVolumeDefinition snapVlmDfn : snapDfn.getAllSnapshotVolumeDefinitions(peerCtx))
                    {
                        futureS3Keys.add(
                            BackupShippingUtils.buildS3VolumeName(
                                rscName,
                                "",
                                snapVlmDfn.getVolumeNumber().value,
                                snapName,
                                s3Suffix
                            )
                        );
                    }
                    String s3KeyShouldLookLikeThis = futureS3Keys.iterator().next();
                    Matcher mtc = BACKUP_VOLUME_PATTERN.matcher(s3KeyShouldLookLikeThis);
                    if (mtc.find())
                    {
                        BackupApi back = fillBackupListPojo(
                            s3KeyShouldLookLikeThis,
                            rscName,
                            snapName,
                            mtc,
                            BACKUP_VOLUME_PATTERN,
                            s3keys,
                            linstorBackupsS3Keys,
                            snapDfn
                        );
                        if (back != null)
                        {
                            retIdToBackupsApiMap.put(s3KeyShouldLookLikeThis, back);
                            linstorBackupsS3Keys.add(s3KeyShouldLookLikeThis);
                        }
                    }
                }
            }
        }

        s3keys.removeAll(linstorBackupsS3Keys);
        return new Pair<>(retIdToBackupsApiMap, s3keys);
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

    private BackupApi fillBackupListPojo(
        String s3key,
        String rscName,
        String snapName,
        Matcher mtc,
        Pattern pattern,
        Set<String> s3keys,
        Set<String> usedKeys,
        SnapshotDefinition snapDfn
    )
    {
        BackupApi back = null;

        String startTime = null;
        Boolean shipping;
        Boolean success;
        Long startTimestamp = null;
        String nodeName = null;
        Map<Integer, BackupVolumePojo> vlms = new TreeMap<>();
        try
        {
            AccessContext peerCtx = peerAccCtx.get();

            {
                int vlmNr = Integer.parseInt(mtc.group(3));
                vlms.put(vlmNr, new BackupVolumePojo(vlmNr, null, null, new BackupVlmS3Pojo(s3key)));
            }
            // get all other keys that start with rscName & contain snapName
            // add them to vlms
            // add them to usedKeys
            for (String otherKey : s3keys)
            {
                if (!usedKeys.contains(otherKey) && !otherKey.equals(s3key))
                {
                    Matcher matcher = pattern.matcher(otherKey);
                    if (
                        matcher.matches() && otherKey.startsWith(rscName) &&
                            otherKey.contains(snapName)
                    )
                    {
                        int vlmNr = Integer.parseInt(matcher.group(3));
                        vlms.put(vlmNr, new BackupVolumePojo(vlmNr, null, null, new BackupVlmS3Pojo(s3key)));
                        usedKeys.add(otherKey);
                    }
                }
            }
            if (snapDfn != null && snapDfn.getFlags().isSet(peerCtx, SnapshotDefinition.Flags.BACKUP))
            {
                startTime = snapName.substring(S3Consts.SNAP_PREFIX_LEN);
                String ts = snapDfn.getProps(peerCtx)
                    .getProp(InternalApiConsts.KEY_BACKUP_START_TIMESTAMP, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                if (ts == null || ts.isEmpty())
                {
                    throw new ImplementationError(
                        "Snapshot " + snapDfn.getName().displayValue +
                            " has the BACKUP-flag set, but does not have a required internal property set."
                    );
                }
                startTimestamp = Long.parseLong(ts); // fail fast
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

        back = new BackupPojo(
            rscName + "_" + snapName,
            rscName,
            snapName,
            startTime,
            startTimestamp,
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

        return back;
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
        String lastBackup,
        Map<String, String> storPoolMapRef,
        String nodeName,
        String remoteName
    )
    {
        return freeCapacityFetcher
            .fetchThinFreeCapacities(
                Collections.emptySet()
            ).flatMapMany(
                ignored -> scopeRunner.fluxInTransactionalScope(
                    "restore backup", lockGuardFactory.buildDeferred(
                        LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP
                    ),
                    () -> backupInfoInTransaction(
                        srcRscName,
                        lastBackup,
                        storPoolMapRef,
                        nodeName,
                        remoteName
                    )
                )
            );
    }

    private Flux<BackupInfoPojo> backupInfoInTransaction(
        String srcRscName,
        String lastBackup,
        Map<String, String> renameMap,
        String nodeName,
        String remoteName
    ) throws AccessDeniedException, InvalidNameException
    {
        S3Remote remote = getS3Remote(remoteName);
        String metaFileName = "";
        byte[] masterKey = getLocalMasterKey();
        String curSrcRscName = srcRscName;
        if (lastBackup != null && !lastBackup.isEmpty())
        {
            Matcher volM = BACKUP_VOLUME_PATTERN.matcher(lastBackup);
            if (volM.matches())
            {
                curSrcRscName = volM.group(1);
                metaFileName = volM.group(1) + volM.group(4) + volM.group(5) + META_SUFFIX;
            }
            else
            {
                Matcher metaM = META_FILE_PATTERN
                    .matcher(lastBackup.endsWith(META_SUFFIX) ? lastBackup : lastBackup + META_SUFFIX);
                if (metaM.matches())
                {
                    curSrcRscName = metaM.group(1);
                    metaFileName = lastBackup.endsWith(META_SUFFIX) ? lastBackup : lastBackup + META_SUFFIX;
                }
                else
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                            "The target backup " + lastBackup +
                                " is invalid since it does not match the pattern of " +
                                "'<rscName>_back_YYYYMMDD_HHMMSS<optional-backup-s3-suffix> " +
                                "(e.g. my-rsc_back_20210824_072543)" +
                                META_FILE_PATTERN + "'. " +
                                "Please provide a valid target backup, or provide only the source resource name " +
                                "to restore to the latest backup of that resource."
                        )
                    );
                }
            }
        }
        List<S3ObjectSummary> objects = backupHandler.listObjects(curSrcRscName, remote, sysCtx, masterKey);
        Set<String> s3keys = objects.stream().map(S3ObjectSummary::getKey)
            .collect(Collectors.toCollection(TreeSet::new));

        if (metaFileName.isEmpty())
        {
            try
            {
                metaFileName = getLatestMetaFile(s3keys, null);
                if (metaFileName == null)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN | ApiConsts.MASK_BACKUP,
                            "Could not find the needed meta-file with the name '" + metaFileName + "' in remote '" +
                                remoteName + "'"
                        )
                    );
                }
            }
            catch (ParseException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP, "Tried to parse s3 key " + metaFileName
                    ),
                    exc
                );
            }
        }
        else
        {
            if (!s3keys.contains(metaFileName))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN | ApiConsts.MASK_BACKUP,
                        "Could not find the needed meta-file with the name '" + metaFileName + "' in remote '" +
                            remoteName + "'"
                    )
                );
            }
        }

        String fullBackup = null;
        String latestBackup = metaFileName.substring(0, metaFileName.length() - S3Consts.META_SUFFIX_LEN);
        LinkedList<BackupMetaDataPojo> data = new LinkedList<>();
        try
        {
            do
            {
                BackupMetaDataPojo metadata = backupHandler.getMetaFile(
                    metaFileName, remote, peerAccCtx.get(), masterKey
                );
                data.add(metadata);
                if (metadata.getBasedOn() == null)
                {
                    fullBackup = metaFileName.substring(0, metaFileName.length() - S3Consts.META_SUFFIX_LEN);
                }
                metaFileName = metadata.getBasedOn();
            }
            while (metaFileName != null);
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                    "Failed to parse meta file " + metaFileName
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
            curSrcRscName,
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
                    Matcher mtc = BACKUP_VOLUME_PATTERN.matcher(backup.getName());
                    if (mtc.matches())
                    {
                        boolean empty = mtc.group(2) == null || mtc.group(2).isEmpty();
                        if (
                            empty && layerData.getRscNameSuffix().isEmpty() ||
                                !empty && mtc.group(2).equals(layerData.getRscNameSuffix())
                        )
                        {
                            vlmName = backup.getName();
                            break;
                        }
                    }
                    else
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
                            l2lCleanupFlux = cleanupStltRemote((StltRemote) remote)
                                .concatWith(
                                    ctrlSatelliteUpdateCaller.updateSatellites(
                                        snapDfn,
                                        CtrlSatelliteUpdateCaller.notConnectedWarn()
                                    ).transform(
                                        responses -> CtrlResponseUtils.combineResponses(
                                            responses,
                                            LinstorParsingUtils.asRscName(rscNameRef),
                                            "Removing remote property from snapshot '" + snapNameRef + "' of {1} on {0}"
                                        )
                                    )
                                );
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
                        if (successRef)
                        {
                            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
                            rscDfn.getProps(peerAccCtx.get()).setProp(
                                InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                                snapNameRef,
                                ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" + remoteName
                            );

                            Remote remote = remoteRepo.get(sysCtx, new RemoteName(remoteName, true));
                            if (remote != null && remote instanceof StltRemote)
                            {
                                cleanupFlux = cleanupStltRemote((StltRemote) remote);
                            }
                        }
                    }
                    ctrlTransactionHelper.commit();
                    startStltCleanup(peerProvider.get(), rscNameRef, snapNameRef);
                    cleanupFlux = cleanupFlux.concatWith(
                        ctrlSatelliteUpdateCaller.updateSatellites(
                            snapDfn,
                            CtrlSatelliteUpdateCaller.notConnectedWarn()
                        ).transform(
                            responses -> CtrlResponseUtils.combineResponses(
                                responses,
                                LinstorParsingUtils.asRscName(rscNameRef),
                                "Finishing shipping of backup ''" + snapNameRef + "'' of {1} on {0}"
                            )
                        )
                    );
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
        byte[] msg = stltComSerializer.headerlessBuilder().notifyBackupShippingFinished(rscNameRef, snapNameRef)
            .build();
        peer.apiCall(InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_FINISHED, msg);
    }

    private Flux<ApiCallRc> cleanupStltRemote(StltRemote remote)
    {
        Flux<ApiCallRc> flux;
        try
        {
            remote.getFlags().enableFlags(sysCtx, StltRemote.Flags.DELETE);
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
