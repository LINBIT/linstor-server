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
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
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
    private static final Pattern SNAP_DFN_TIME_PATTERN = Pattern
        .compile("^(?:back_(?:inc_)?)([0-9]{8}_[0-9]{6})");
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
        CtrlStltSerializer ctrlComSerializerRef
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
        boolean incremental,
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
                        "Backup shipping requires a set up encryption. Please use 'linstor encryption create-passphrase' or '... enter-passphrase'"
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
            if (incremental)
            {
                if (prevSnapName == null)
                {
                    errorReporter.logWarning(
                        "Could not create an incremental backup for resource %s as there is no previous full backup. Creating a full backup instead.",
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
                            "Could not create an incremental backup for resource %s as the previous snapshot %s needed for the incremental backup has already been deleted. Creating a full backup instead.",
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
                                    "Current vlmDfn size (%d) does not match with prev snapDfn (%s) size (%d). Forcing full backup.",
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

            // save the s3 suffix as prop so that when restoring the satellite can reconstruct the .meta name (s3 suffix
            // is NOT part of snapshot name)
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
                                "' cannot be started since there is no support for mixing external and internal drbd-metadata among volumes."
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
                "Shipping of resource " + rscNameRef + " to remote " + remoteName + " in progress.",
                ApiConsts.MASK_INFO
            );

            Flux<ApiCallRc> flux = snapshotCrtHandler.postCreateSnapshot(snapDfn)
                .concatWith(Flux.<ApiCallRc> just(responses));
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
            if (!id.endsWith(META_SUFFIX))
            {
                id += META_SUFFIX;
            }
            deleteByIdPrefix(
                id,
                false,
                cascading,
                s3LinstorObjects,
                s3Remote,
                toDelete
            );
        }
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
        else if (s3Key != null && !s3Key.isEmpty()) // case 3: s3Key [cascading]
        {
            deleteByS3Key(s3LinstorObjects, Collections.singleton(s3Key), cascading, toDelete);
            toDelete.s3keys.add(s3Key);
            toDelete.s3KeysNotFound.remove(s3Key); // ignore this
        }
        else if (
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
        else if (all) // case 5: all // force cascading
        {
            deleteByS3Key(
                s3LinstorObjects,
                s3LinstorObjects.keySet(),
                true,
                toDelete
            );
        }
        else if (allLocalCluster) // case 6: allCluster // forced cascading
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
        Predicate<String> nodeNameCheck = nodeNameRef == null || nodeNameRef.isEmpty() ? ignore -> true
            : nodeNameRef::equalsIgnoreCase;
        Predicate<String> rscNameCheck = rscNameRef == null || rscNameRef.isEmpty() ? ignore -> true
            : rscNameRef::equalsIgnoreCase;
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
                    Matcher m = META_FILE_PATTERN.matcher(s3Obj.s3Key);
                    if (!m.find())
                    {
                        throw new ImplementationError("Unexpected meta filename");
                    }
                    timeStr = m.group(2).substring(S3Consts.SNAP_PREFIX_LEN);
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
                    for (List<BackupInfoPojo> backupInfoPojoList : s3MetaFile.getBackups().values())
                    {
                        for (BackupInfoPojo backupInfoPojo : backupInfoPojoList)
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
        String lastBackup
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
                        lastBackup
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
        String lastBackup
    ) throws AccessDeniedException, InvalidNameException
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        Date targetTime = null;
        String shortTargetName = null;
        if (lastBackup != null && !lastBackup.isEmpty())
        {
            if (!lastBackup.endsWith(META_SUFFIX))
            {
                lastBackup = lastBackup + META_SUFFIX;
            }
            Matcher targetMatcher = META_FILE_PATTERN.matcher(lastBackup);
            if (targetMatcher.matches())
            {
                srcRscName = targetMatcher.group(1);
                String snapName = targetMatcher.group(2);
                shortTargetName = targetMatcher.group(1) + "_" + snapName;
                try
                {
                    targetTime = S3Consts.DATE_FORMAT.parse(snapName.substring(S3Consts.SNAP_PREFIX_LEN));
                }
                catch (ParseException exc)
                {
                    errorReporter.reportError(exc, peerAccCtx.get(), null, "used s3 key: " + lastBackup);
                }
            }
            else
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                        "The target backup " + lastBackup + META_SUFFIX +
                            " is invalid since it does not match the pattern of '<rscName>_back_YYYYMMDD_HHMMSS<optional-backup-s3-suffix> (e.g. my-rsc_back_20210824_072543)" +
                            META_FILE_PATTERN + "'. " +
                            "Please provide a valid target backup, or provide only the source resource name to restore to the latest backup of that resource."
                    )
                );
            }
        }
        S3Remote remote = getS3Remote(remoteName);
        byte[] targetMasterKey = getLocalMasterKey();
        // 1. list srcRscName*
        Set<String> s3keys = backupHandler.listObjects(srcRscName, remote, peerAccCtx.get(), targetMasterKey).stream()
            .map(S3ObjectSummary::getKey)
            .collect(Collectors.toCollection(TreeSet::new));
        if (targetTime != null && !s3keys.contains(lastBackup))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                    "The target backup " + lastBackup +
                        " is invalid since it does not exist in the given remote " + remoteName + ". " +
                        "Please provide a valid target backup, or provide only the source resource name to restore to the latest backup of that resource."
                )
            );
        }
        // 2. find meta-file
        String metaName = null;
        Date latestBackTs = null;
        for (String s3key : s3keys)
        {
            Matcher m = META_FILE_PATTERN.matcher(s3key);
            if (m.matches())
            {
                try
                {
                    // remove "back_" prefix
                    String ts = m.group(2).substring(S3Consts.SNAP_PREFIX_LEN);
                    Date curTs = S3Consts.DATE_FORMAT.parse(ts);
                    if (targetTime != null)
                    {
                        if (
                            (latestBackTs == null || latestBackTs.before(curTs)) &&
                                (targetTime.after(curTs) || targetTime.equals(curTs))
                        )
                        {
                            metaName = m.group();
                            latestBackTs = curTs;
                        }
                    }
                    else
                    {
                        if (latestBackTs == null || latestBackTs.before(curTs))
                        {
                            metaName = m.group();
                            latestBackTs = curTs;
                        }
                    }
                }
                catch (ParseException exc)
                {
                    errorReporter.reportError(exc, peerAccCtx.get(), null, "used s3 key: " + s3key);
                }
            }
        }
        if (metaName == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN | ApiConsts.MASK_BACKUP,
                    "Could not find backups with the given resource name '" + srcRscName + "' in remote '" +
                        remoteName + "'"
                )
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
            BackupMetaDataPojo metadata;
            do
            {
                metadata = backupHandler.getMetaFile(metaName, remote, peerAccCtx.get(), targetMasterKey);
                Snapshot snap = createSnapshotByS3Meta(
                    srcRscName,
                    storPoolMap,
                    nodeName,
                    targetRscName,
                    passphrase,
                    shortTargetName,
                    remote,
                    s3keys,
                    latestBackTs,
                    metaName,
                    metadata,
                    responses,
                    resetData
                );
                // all other "basedOn" snapshots should not change props / size / etc..
                resetData = false;
                snap.getProps(peerAccCtx.get()).setProp(
                    InternalApiConsts.KEY_BACKUP_TO_RESTORE,
                    srcRscName + "_" + snap.getSnapshotName().displayValue,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                );
                LinkedList<Snapshot> list = new LinkedList<>();
                backupInfoMgr.backupsToUploadAddEntry(snap, list);
                if (nextBackup != null)
                {
                    list.add(nextBackup);
                }
                nextBackup = snap;
                metaName = metadata.getBasedOn();
            }
            while (metaName != null);
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
                "Restoring backup of resource " + srcRscName + " from remote " + remoteName +
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

    private Snapshot createSnapshotByS3Meta(
        String srcRscName,
        Map<String, String> storPoolMap,
        String nodeName,
        String targetRscName,
        String passphrase,
        String shortTargetName,
        S3Remote remote,
        Set<String> s3keys,
        Date latestBackTs,
        String metaName,
        BackupMetaDataPojo metadata,
        ApiCallRcImpl responses,
        boolean resetData
    )
        throws AccessDeniedException, ImplementationError, DatabaseException, InvalidValueException
    {
        for (List<BackupInfoPojo> backupList : metadata.getBackups().values())
        {
            for (BackupInfoPojo backup : backupList)
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
        Node node = ctrlApiDataLoader.loadNode(nodeName, true);
        if (!node.getPeer(peerAccCtx.get()).isConnected())
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
        Matcher m = META_FILE_PATTERN.matcher(metaName);
        if (!m.find())
        {
            throw new ImplementationError("not a meta file: " + metaName);
        }
        SnapshotName snapName = LinstorParsingUtils.asSnapshotName(m.group(2));
        ResourceDefinition rscDfn = getRscDfnForBackupRestore(targetRscName, snapName, metadata, metaName, resetData);
        // 9. create snapDfn
        SnapshotDefinition snapDfn = getSnapDfnForBackupRestore(metadata, snapName, rscDfn, responses);
        // 10. create vlmDfn(s)
        // 11. create snapVlmDfn(s)
        Map<Integer, SnapshotVolumeDefinition> snapVlmDfns = new TreeMap<>();
        long totalSize = createSnapVlmDfnForBackupRestore(
            targetRscName,
            metadata,
            rscDfn,
            snapDfn,
            snapVlmDfns,
            resetData
        );

        // TODO: check if all storPools have enough space for restore

        // StorPool storPool = ctrlApiDataLoader.loadStorPool(storPoolName, nodeName, true);
        // boolean storPoolUsable = FreeCapacityAutoPoolSelectorUtils.isStorPoolUsable(
        // totalSize,
        // thinFreeCapacities,
        // storPool.getDeviceProviderKind().usesThinProvisioning(),
        // storPool.getName(),
        // node,
        // peerAccCtx.get()
        // ).orElse(true);
        // if (!storPoolUsable)
        // {
        // throw new ApiRcException(
        // ApiCallRcImpl.simpleEntry(
        // ApiConsts.FAIL_INVLD_VLM_SIZE,
        // "Not enough space in storage pool " + storPoolName + " to restore the backup."
        // )
        // );
        // }
        //
        // Map<String, String> renameMap = createRenameMap(layers, storPoolName);

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
        String sourceClusterIdStr,
        String nodeNameStr,
        String storPoolNameStr,
        Map<StorPool.Key, Long> thinFreeCapacities,
        String targetRscName,
        BackupMetaDataPojo metadata,
        Map<String, String> renameMap,
        StltRemote remote,
        SnapshotName snapName,
        Set<String> srcSnapDfnUuidsForIncrementalRef,
        boolean useZstd
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        Flux<BackupShippingStartInfo> ret;

        try
        {
            // 5. create layerPayload
            RscLayerDataApi layers = metadata.getLayerData();

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
                ResourceDefinition targetRscDfn = rscDfnRepo.get(sysCtx, new ResourceName(targetRscName));
                if (targetRscDfn != null)
                {
                    incrementalBaseSnap = getIncrementalBase(
                        targetRscDfn,
                        srcSnapDfnUuidsForIncrementalRef
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
                targetRscName,
                snapName,
                metadata,
                snapName.displayValue,
                resetData
            );
            SnapshotDefinition snapDfn;
            Map<Integer, SnapshotVolumeDefinition> snapVlmDfns = new TreeMap<>();
            Set<StorPool> storPools;
            Snapshot snap = null;

            // 9. create snapDfn
            snapDfn = getSnapDfnForBackupRestore(metadata, snapName, rscDfn, responses);
            // 10. create vlmDfn(s)
            // 11. create snapVlmDfn(s)
            long totalSize = createSnapVlmDfnForBackupRestore(
                targetRscName,
                metadata,
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

                metadata.getRsc().getProps().put(
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
                if (useZstd)
                {
                    extTools.put(ExtTools.ZSTD, null);
                }
                AutoSelectFilterBuilder autoSelectBuilder = new AutoSelectFilterBuilder()
                    .setPlaceCount(1)
                    .setNodeNameList(nodeNameStr == null ? null : Arrays.asList(nodeNameStr))
                    .setStorPoolNameList(storPoolNameStr == null ? null : Arrays.asList(storPoolNameStr))
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
                if ((storPools == null || storPools.isEmpty()) && useZstd)
                {
                    // try not using it..
                    useZstd = false;
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
                if (snap == null)
                {
                    // otherwise we found and reuse snapshot for incremental shipping
                    StorPool storPool = storPools.iterator().next();
                    Node node = storPool.getNode();

                    // TODO add autoSelected storPool into renameMap

                    // 12. create snapshot
                    snap = createSnapshotAndVolumesForBackupRestore(
                        metadata,
                        layers,
                        node,
                        snapDfn,
                        snapVlmDfns,
                        renameMap,
                        remote,
                        null
                    );
                }
                snap.getProps(peerAccCtx.get()).setProp(
                    InternalApiConsts.KEY_BACKUP_TO_RESTORE,
                    snap.getResourceName().displayValue + "_" + snap.getSnapshotName().displayValue,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                );
                snap.getFlags().enableFlags(peerAccCtx.get(), Snapshot.Flags.BACKUP_TARGET);

                // LUKS
                Set<AbsRscLayerObject<Snapshot>> luksLayerData = LayerRscUtils.getRscDataByProvider(
                    snap.getLayerData(sysCtx),
                    DeviceLayerKind.LUKS
                );
                if (!luksLayerData.isEmpty())
                {
                    LuksLayerMetaPojo luksInfo = metadata.getLuksInfo();
                    if (luksInfo == null)
                    {
                        throw new ImplementationError("Cannot receive LUKS data without LuksInfo");
                    }
                    byte[] remoteMasterKey = getRemoteMasterKey(sourceClusterIdStr, luksInfo);
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

                remote.useZstd(sysCtx, useZstd);

                // update stlts
                ctrlTransactionHelper.commit();

                // calling ...SupressingErrorClasses without classes => do not ignore DelayedApiRcException. we want to
                // deal with that by our self
                ret = ctrlSatelliteUpdateCaller.updateSatellites(remote)
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
    )
    {
        backupInfoMgr.restoreRemoveEntry(snapDfnRef.getResourceDefinition());
        ctrlTransactionHelper.commit();

        return Flux.error(throwableRef);
    }

    private long getTotalSize(ResourceDefinition rscDfn)
    {
        long totalSize = 0;
        try
        {
            Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn(sysCtx);
            while (vlmDfnIt.hasNext())
            {
                VolumeDefinition vlmDfn = vlmDfnIt.next();
                totalSize += vlmDfn.getVolumeSize(sysCtx);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return totalSize;
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

        LinkedList<String> backups = new LinkedList<>();
        for (List<BackupInfoPojo> backupList : metadata.getBackups().values())
        {
            for (BackupInfoPojo backup : backupList)
            {
                String name = backup.getName();
                Matcher m = BACKUP_VOLUME_PATTERN.matcher(name);
                m.matches();
                String shortName = m.group(1) + "_" + m.group(4);
                backups.add(shortName);
                if (shortTargetName != null && shortTargetName.equals(shortName))
                {
                    break;
                }
            }
        }

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
        ApiCallRcImpl responsesRef
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
                        "After downloading the Backup Linstor will NOT restore the data to prevent unintentional data-loss."
                )
            );
        }
        else
        {
            snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.RESTORE_BACKUP_ON_SUCCESS);
        }

        return snapDfn;
    }

    private ResourceDefinition getRscDfnForBackupRestore(
        String targetRscName,
        SnapshotName snapName,
        BackupMetaDataPojo metadata,
        String metaName,
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

                    Map<Integer, List<BackupInfoPojo>> s3MetaVlmMap = s3MetaFile.getBackups();
                    Map<Integer, BackupVolumePojo> retVlmPojoMap = new TreeMap<>(); // vlmNr, vlmPojo
                    boolean restorable = true;

                    for (Entry<Integer, List<BackupInfoPojo>> entry : s3MetaVlmMap.entrySet())
                    {
                        Integer s3MetaVlmNr = entry.getKey();
                        List<BackupInfoPojo> s3BackVlmInfoList = entry.getValue();
                        for (BackupInfoPojo s3BackVlmInfo : s3BackVlmInfoList)
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
                                    if (s3MetaVlmNr == s3VlmNrFromBackupName)
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
                Matcher m = BACKUP_VOLUME_PATTERN.matcher(s3key);
                if (m.matches())
                {
                    String rscName = m.group(1);
                    String snapName = m.group(4);

                    SnapshotDefinition snapDfn = loadSnapDfnIfExists(rscName, snapName);

                    BackupApi back = fillBackupListPojo(
                        s3key,
                        rscName,
                        snapName,
                        m,
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
                    Matcher m = BACKUP_VOLUME_PATTERN.matcher(s3KeyShouldLookLikeThis);
                    if (m.find())
                    {
                        BackupApi back = fillBackupListPojo(
                            s3KeyShouldLookLikeThis,
                            rscName,
                            snapName,
                            m,
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
        Matcher m,
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
                int vlmNr = Integer.parseInt(m.group(3));
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

    private Flux<ApiCallRc> backupAbortInTransaction(String rscNameRef, boolean restore, boolean create)
        throws AccessDeniedException, DatabaseException
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
        Set<SnapshotDefinition> snapDfns = getInProgressBackups(rscDfn);
        if (snapDfns.isEmpty())
        {
            return Flux.empty();
        }
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

        Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateStlts = Flux.empty();
        for (SnapshotDefinition snapDfn : snapDfns)
        {
            Collection<Snapshot> snaps = snapDfn.getAllSnapshots(peerAccCtx.get());
            boolean abort = false;
            for (Snapshot snap : snaps)
            {
                if (
                    snap.getFlags().isSet(peerAccCtx.get(), Snapshot.Flags.BACKUP_TARGET) && restore ||
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
                updateStlts = updateStlts.concatWith(
                    ctrlSatelliteUpdateCaller.updateSatellites(snapDfn, CtrlSatelliteUpdateCaller.notConnectedWarn())
                );
            }
        }

        ctrlTransactionHelper.commit();
        ApiCallRcImpl success = new ApiCallRcImpl();
        success.addEntry(
            "Successfully aborted all " +
                (create && restore ? "in-progress backup-shipments and restores"
                    : create ? "in-progress backup-shipments" : "in-progress backup-restores") +
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

    public Flux<ApiCallRc> shippingReceived(String rscNameRef, String snapNameRef, boolean successRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Finish receiving backup",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> shippingReceivedInTransaction(rscNameRef, snapNameRef, successRef)
            );
    }

    private Flux<ApiCallRc> shippingReceivedInTransaction(String rscNameRef, String snapNameRef, boolean successRef)
    {
        errorReporter.logInfo(
            "Backup receiving for snapshot %s of resource %s %s", snapNameRef, rscNameRef,
            successRef ? "finished successfully" : "failed"
        );
        Flux<ApiCallRc> flux;
        SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameRef, snapNameRef, true);
        try
        {
            AccessContext peerCtx = peerAccCtx.get();

            Snapshot snap = snapDfn.getSnapshot(peerCtx, peerProvider.get().getNode().getName());
            snapDfn.getFlags().disableFlags(peerCtx, SnapshotDefinition.Flags.SHIPPING);
            snapDfn.getFlags().enableFlags(peerCtx, SnapshotDefinition.Flags.SHIPPED);
            snap.getProps(peerCtx).removeProp(
                InternalApiConsts.KEY_BACKUP_SRC_REMOTE,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
            snap.getProps(peerCtx).removeProp(
                InternalApiConsts.KEY_BACKUP_TO_RESTORE,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
            snap.getFlags().disableFlags(peerCtx, Snapshot.Flags.BACKUP_TARGET);

            boolean keepGoing;

            Snapshot nextSnap = backupInfoMgr.getNextBackupToUpload(snap);
            if (successRef && nextSnap != null)
            {
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
                backupInfoMgr.restoreRemoveEntry(snapDfn.getResourceDefinition());
                if (successRef)
                {
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
                    flux = ctrlSnapDeleteApiCallHandler.deleteSnapshot(
                        snapDfn.getResourceName().displayValue,
                        snapDfn.getName().displayValue,
                        null
                    );
                    keepGoing = false; // last backup failed.
                }
            }

            Flux<ApiCallRc> l2lCleanupFlux = Flux.empty();
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

            ctrlTransactionHelper.commit();
            startStltCleanup(peerProvider.get(), rscNameRef, snapNameRef);
            return l2lCleanupFlux.concatWith(flux);
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

    public Flux<ApiCallRc> shippingSent(String rscNameRef, String snapNameRef, boolean successRef)
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
        SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameRef, snapNameRef, true);
        try
        {
            Flux<ApiCallRc> cleanupFlux = Flux.empty();
            NodeName nodeName = peerProvider.get().getNode().getName();
            backupInfoMgr.abortDeleteEntries(nodeName.displayValue, rscNameRef, snapNameRef);
            if (!successRef && snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING_ABORT))
            {
                // re-enable shipping-flag to make sure the abort-logic gets triggered later on
                snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
                ctrlTransactionHelper.commit();
                startStltCleanup(peerProvider.get(), rscNameRef, snapNameRef);
                return ctrlSnapShipAbortHandler.abortBackupShippingPrivileged(snapDfn.getResourceDefinition());
            }
            snapDfn.getFlags().disableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
            if (successRef)
            {
                snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED);
            }
            Snapshot snap = snapDfn.getSnapshot(peerAccCtx.get(), nodeName);
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
            ctrlTransactionHelper.commit();
            startStltCleanup(peerProvider.get(), rscNameRef, snapNameRef);
            return cleanupFlux.concatWith(
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
        catch (AccessDeniedException | InvalidNameException | InvalidValueException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
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
            if (!deviceProviderKind.isSnapshotShippingSupported())
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
                continue;
            }
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
        ExtToolsInfo info = extToolsManagerRef.getExtToolInfo(extTool);
        if (info == null || !info.isSupported())
        {
            return ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_SNAPSHOT_SHIPPING_NOT_SUPPORTED,
                String.format(
                    "%s based backup shipping requires support for %s",
                    deviceProviderKind.name(),
                    toolDescr
                ),
                true
            );
        }
        if (version != null && !info.hasVersionOrHigher(version))
        {
            return ApiCallRcImpl.simpleEntry(
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
        return null;
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
