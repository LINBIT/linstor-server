package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.BackupToS3;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.backups.BackupInfoPojo;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.api.pojo.backups.BackupPojo;
import com.linbit.linstor.api.pojo.backups.BackupPojo.BackupS3Pojo;
import com.linbit.linstor.api.pojo.backups.BackupPojo.BackupVlmS3Pojo;
import com.linbit.linstor.api.pojo.backups.BackupPojo.BackupVolumePojo;
import com.linbit.linstor.api.pojo.backups.LuksLayerMetaPojo;
import com.linbit.linstor.api.pojo.backups.VlmDfnMetaPojo;
import com.linbit.linstor.api.pojo.backups.VlmMetaPojo;
import com.linbit.linstor.backupshipping.BackupShippingConsts;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
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
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.S3Remote;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition.Flags;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
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
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.ExceptionThrowingPredicate;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
        .compile("^([a-zA-Z0-9_-]{2,48})_(back_[0-9]{8}_[0-9]{6})\\.meta$");
    private static final Pattern BACKUP_KEY_PATTERN = Pattern
        .compile("^([a-zA-Z0-9_-]{2,48})(\\..+)?_([0-9]{5})_(back_[0-9]{8}_[0-9]{6})$");
    private final Provider<AccessContext> peerAccCtx;
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

    @Inject
    public CtrlBackupApiCallHandler(
        @PeerContext Provider<AccessContext> peerAccCtxRef,
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
        ResourceDefinitionRepository rscDfnRepoRef
    )
    {
        peerAccCtx = peerAccCtxRef;
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
    }

    public Flux<ApiCallRc> createBackup(
        String rscNameRef,
        String remoteNameRef,
        String nodeNameRef,
        boolean incremental
    )
        throws AccessDeniedException
    {
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
                incremental
            )
        );

        return response;
    }

    private Flux<ApiCallRc> backupSnapshot(String rscNameRef, String remoteName, String nodeName, boolean incremental)
        throws AccessDeniedException
    {
        Date now = new Date();
        String snapName = BackupShippingConsts.SNAP_PREFIX + BackupApi.DATE_FORMAT.format(now);
        try
        {
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
            S3Remote remote = getS3Remote(remoteName);
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
            SnapshotDefinition prevSnap = null;
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
                    prevSnap = ctrlApiDataLoader.loadSnapshotDfn(
                        rscDfn,
                        new SnapshotName(prevSnapName),
                        false
                    );
                    if (prevSnap == null)
                    {
                        errorReporter.logWarning(
                            "Could not create an incremental backup for resource %s as the previous snapshot %s needed for the incremental backup has already been deleted. Creating a full backup instead.",
                            rscNameRef, prevSnapName
                        );
                        incremental = false;
                    }
                }
            }
            ApiCallRcImpl responses = new ApiCallRcImpl();

            Pair<Node, List<String>> chooseNodeResult = chooseNode(rscDfn, prevSnap);
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
            if (!incremental)
            {
                snapDfn.getProps(peerAccCtx.get())
                    .setProp(
                        InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP, snapName, ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );
            }
            else
            {
                // incremental == true && prevSnap == null should not be possible here
                snapDfn.getProps(peerAccCtx.get())
                    .setProp(
                        InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP,
                        prevSnap.getProps(peerAccCtx.get()).getProp(
                            InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP,
                            ApiConsts.NAMESPC_BACKUP_SHIPPING
                        ),
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
                        nodeIds.add(rscData.getNodeId().value);
                    }
                }
            }

            snapDfn.getSnapshot(peerAccCtx.get(), chosenNodeName).getFlags()
                .enableFlags(peerAccCtx.get(), Snapshot.Flags.BACKUP_SOURCE);
            Props snapProps = snapDfn.getSnapshot(peerAccCtx.get(), chosenNodeName).getProps(peerAccCtx.get());
            snapProps.setProp(
                InternalApiConsts.KEY_BACKUP_NODE_IDS_TO_RESET,
                StringUtils.join(nodeIds, InternalApiConsts.KEY_BACKUP_NODE_ID_SEPERATOR),
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
            snapProps.setProp(
                InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                remote.getName().displayValue,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
            if (incremental)
            {
                snapProps.setProp(
                    InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                    prevSnapName,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                );
            }
            ctrlTransactionHelper.commit();
            return Flux.<ApiCallRc>just(responses)
                .concatWith(snapshotCrtHandler.postCreateSnapshot(snapDfn));
        }
        catch (InvalidNameException exc)
        {
            // ignored
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        return Flux.empty();
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
     *
     * @return
     *
     * @throws AccessDeniedException
     */
    private Pair<Node, List<String>> chooseNode(ResourceDefinition rscDfn, SnapshotDefinition prevSnapDfnRef)
        throws AccessDeniedException
    {
        Node selectedNode = null;
        List<String> nodeNamesStr = new ArrayList<>();

        ExceptionThrowingPredicate<Node, AccessDeniedException> shouldNodeCreateSnapshot;
        if (prevSnapDfnRef == null)
        {
            shouldNodeCreateSnapshot = ignored -> true;
        }
        else
        {
            shouldNodeCreateSnapshot = node -> prevSnapDfnRef.getAllSnapshots(peerAccCtx.get()).stream()
                .anyMatch(snap -> snap.getNode().equals(node));
        }

        Iterator<Resource> rscIt = rscDfn.iterateResource(peerAccCtx.get());
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            if (
                !rsc.getStateFlags()
                    .isSomeSet(peerAccCtx.get(), Resource.Flags.DRBD_DISKLESS, Resource.Flags.NVME_INITIATOR) &&
                    backupShippingSupported(rsc).isEmpty()
            )
            {
                Node node = rsc.getNode();
                if (shouldNodeCreateSnapshot.test(node))
                {
                    if (selectedNode == null)
                    {
                        selectedNode = node;
                    }
                    nodeNamesStr.add(node.getName().displayValue);
                }
            }
        }
        if (nodeNamesStr.size() == 0)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_ENOUGH_NODES,
                    "Backup shipping of resource '" + rscDfn.getName().displayValue +
                        "' cannot be started since there is no node available that supports backup shipping."
                )
            );
        }
        return new Pair<>(selectedNode, nodeNamesStr);
    }

    public Flux<ApiCallRc> deleteBackup(
        String rscName,
        String id,
        String idPrefix,
        String timestamp,
        String nodeName,
        boolean cascading,
        boolean allCluster,
        boolean all,
        String s3key,
        String remoteName,
        boolean dryRunRef
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Backup snapshot",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> deleteBackupInTransaction(
                id,
                idPrefix,
                cascading,
                s3key,
                rscName,
                nodeName,
                timestamp,
                allCluster,
                all,
                remoteName,
                dryRunRef
            )
        );
    }

    private Flux<ApiCallRc> deleteBackupInTransaction(
        String id,
        String idPrefix,
        boolean cascading,
        String s3Key,
        String rscName,
        String nodeName,
        String timestamp,
        boolean allLocalCluster,
        boolean all,
        String remoteName,
        boolean dryRunRef
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
            if (!id.endsWith(".meta"))
            {
                id += ".meta";
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
        else if (timestamp != null && !timestamp.isEmpty() ||
            rscName != null && !rscName.isEmpty() ||
            nodeName != null && !nodeName.isEmpty()) // case 4: (time|rsc|node)+ [cascading]
        {
            deleteByTimeRscNode(
                s3LinstorObjects,
                timestamp,
                rscName,
                nodeName,
                cascading,
                s3Remote,
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

        if (dryRunRef)
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
                    ctrlSnapDeleteApiCallHandler
                        .deleteSnapshot(snapKey.getResourceName().displayValue, snapKey.getSnapshotName().displayValue)
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
                    "' not found on remote '" +
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
        S3Remote s3RemoteRef,
        ToDeleteCollections toDeleteRef
    )
        throws AccessDeniedException
    {

        Predicate<String> nodeNameCheck = nodeNameRef == null || nodeNameRef.isEmpty() ?
            ignore -> true :
            nodeNameRef::equalsIgnoreCase;
        Predicate<String> rscNameCheck = rscNameRef == null || rscNameRef.isEmpty() ?
            ignore -> true :
            rscNameRef::equalsIgnoreCase;
        Predicate<Long> timestampCheck;
        if (timestampRef == null || timestampRef.isEmpty())
        {
            timestampCheck = ignore -> true;
        }
        else
        {
            try
            {
                Date date = BackupApi.DATE_FORMAT.parse(timestampRef);
                timestampCheck = timestamp -> date.after(new Date(timestamp));
            }
            catch (ParseException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_TIME_PARAM,
                        "Failed to parse '" + timestampRef + "'. Expected format: YYYYMMDD_HHMMSS"
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
                long startTimestamp = s3Obj.metaFile.getStartTimestamp();

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

                    for (BackupInfoPojo backupInfoPojo : s3MetaFile.getBackups().values())
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
                Matcher s3BackupKeyMatcher = BACKUP_KEY_PATTERN.matcher(s3Key);
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
                        srcRscName, storPoolMapRef, nodeName, thinFreeCapacities, targetRscName, remoteName,
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
        Date targetTime = null;
        String shortTargetName = null;
        if (lastBackup != null && !lastBackup.isEmpty())
        {
            Matcher targetMatcher = BACKUP_KEY_PATTERN.matcher(lastBackup);
            if (targetMatcher.matches())
            {
                srcRscName = targetMatcher.group(1);
                shortTargetName = targetMatcher.group(1) + "_" + targetMatcher.group(4);
                try
                {
                    targetTime = BackupApi.DATE_FORMAT.parse(targetMatcher.group(4));
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
                        "The target backup " + lastBackup +
                            " is invalid since it does not match the pattern of rscName_vlmNr_YYYYMMDD_HHMMSS. " +
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
        Date latestBackTs = null;
        for (String s3key : s3keys)
        {
            Matcher m = META_FILE_PATTERN.matcher(s3key);
            if (m.matches())
            {
                try
                {
                    // remove "back_" prefix
                    String ts = m.group(2).substring(BackupShippingConsts.SNAP_PREFIX_LEN);
                    Date curTs = BackupApi.DATE_FORMAT.parse(ts);
                    if (targetTime != null)
                    {
                        if (
                            (latestBackTs == null || latestBackTs.before(curTs)) &&
                            (targetTime.after(curTs) || targetTime.equals(curTs))
                        )
                        {
                            latestBackTs = curTs;
                        }
                    }
                    else
                    {
                        if (latestBackTs == null || latestBackTs.before(curTs))
                        {
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
        String metaName = srcRscName + "_back_" + BackupApi.DATE_FORMAT.format(latestBackTs) + ".meta";
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
                    metadata
                );
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
            } while (metaName != null);

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

            return snapshotCrtHandler.postCreateSnapshot(nextBackup.getSnapshotDefinition());
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP, "Failed to parse meta file " + metaName
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
        BackupMetaDataPojo metadata
    )
        throws AccessDeniedException, ImplementationError, DatabaseException, InvalidValueException
    {
        for (BackupInfoPojo backup : metadata.getBackups().values())
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
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(targetRscName, false);
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();
        SnapshotName snapName = LinstorParsingUtils.asSnapshotName(
            BackupShippingConsts.SNAP_PREFIX + BackupApi.DATE_FORMAT.format(metadata.getStartTimestamp())
        );
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
        else if (rscDfn.getResourceCount() != 0)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_RSC | ApiConsts.MASK_BACKUP,
                    "Cannot restore to resource definition which already has resources"
                )
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
        rscDfn.getFlags()
            .resetFlagsTo(
                peerAccCtx.get(), ResourceDefinition.Flags.restoreFlags(metadata.getRscDfn().getFlags())
            );
        rscDfn.getProps(peerAccCtx.get()).clear();
        rscDfn.getProps(peerAccCtx.get()).map().putAll(metadata.getRscDfn().getProps());

        // force the node to become primary afterwards in case we needed to recreate
        // the metadata
        rscDfn.getProps(peerAccCtx.get()).removeProp(InternalApiConsts.PROP_PRIMARY_SET);

        // 9. create snapDfn
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
        // 10. create vlmDfn(s)
        // 11. create snapVlmDfn(s)
        Map<Integer, SnapshotVolumeDefinition> snapVlmDfns = new TreeMap<>();
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
            else
            {
                vlmDfn.getFlags().resetFlagsTo(
                    peerAccCtx.get(), VolumeDefinition.Flags.restoreFlags(vlmDfnMetaEntry.getValue().getFlags())
                );
                vlmDfn.setVolumeSize(peerAccCtx.get(), vlmDfnMetaEntry.getValue().getSize());
            }
            vlmDfn.getProps(peerAccCtx.get()).map().putAll(vlmDfnMetaEntry.getValue().getProps());
            totalSize += vlmDfnMetaEntry.getValue().getSize();
            SnapshotVolumeDefinition snapVlmDfn = snapshotCrtHelper.createSnapshotVlmDfnData(snapDfn, vlmDfn);
            snapVlmDfn.getProps(peerAccCtx.get()).map().putAll(vlmDfnMetaEntry.getValue().getProps());
            snapVlmDfns.put(vlmDfnMetaEntry.getKey(), snapVlmDfn);
        }
        // check if all storPools have enough space for restore
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
        // // 12. create snapshot
        // Map<String, String> renameMap = createRenameMap(layers, storPoolName);
        Snapshot snap = snapshotCrtHelper
            .restoreSnapshot(snapDfn, node, layers, storPoolMap);
        Props snapProps = snap.getProps(peerAccCtx.get());

        LinkedList<String> backups = new LinkedList<>();
        for (BackupInfoPojo backup : metadata.getBackups().values())
        {
            String name = backup.getName();
            Matcher m = BACKUP_KEY_PATTERN.matcher(name);
            m.matches();
            String shortName = m.group(1) + "_" + m.group(4);
            backups.add(shortName);
            if (shortTargetName != null && shortTargetName.equals(shortName))
            {
                break;
            }
        }

        snapProps.map().putAll(metadata.getRsc().getProps());
        snapProps.setProp(
            InternalApiConsts.KEY_BACKUP_SRC_REMOTE,
            remote.getName().displayValue,
            ApiConsts.NAMESPC_BACKUP_SHIPPING
        );

        List<DeviceLayerKind> usedDeviceLayerKinds = LayerUtils.getUsedDeviceLayerKinds(
            snap.getLayerData(peerAccCtx.get()), peerAccCtx.get()
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
        // 13. create snapshotVlm(s)
        for (Entry<Integer, VlmMetaPojo> vlmMetaEntry : metadata.getRsc().getVlms().entrySet())
        {
            SnapshotVolume snapVlm = snapshotCrtHelper
                .restoreSnapshotVolume(layers, snap, snapVlmDfns.get(vlmMetaEntry.getKey()), storPoolMap);
            snapVlm.getProps(peerAccCtx.get()).map()
                .putAll(metadata.getRsc().getVlms().get(vlmMetaEntry.getKey()).getProps());
        }
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

    private Map<String, String> createRenameMap(RscLayerDataApi layers, String targetStorPool)
    {
        Map<String, String> renameMap = new TreeMap<>();
        if (layers.getLayerKind().equals(DeviceLayerKind.STORAGE))
        {
            for (VlmLayerDataApi vlm : layers.getVolumeList())
            {
                renameMap.put(vlm.getStorPoolApi().getStorPoolName(), targetStorPool);
            }
        }

        for (RscLayerDataApi child : layers.getChildren())
        {
            if (child.getRscNameSuffix().equals(RscLayerSuffixes.SUFFIX_DATA))
            {
                renameMap.putAll(createRenameMap(child, targetStorPool));
            }
        }
        return renameMap;
    }

    /**
     * @return
     * <code>Pair.objA</code>: Map of s3Key -> backupApi <br />
     * <code>Pair.objB</code>: Set of s3Keys that are either corrupt metafiles or not known to linstor
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
         *
         * This map will look like follows:
         * "" -> [full]
         * "full" -> [inc1, inc3]
         * "inc1" -> [inc2]
         *
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

                    Map<Integer, BackupInfoPojo> s3MetaVlmMap = s3MetaFile.getBackups();
                    Map<Integer, BackupVolumePojo> retVlmPojoMap = new TreeMap<>(); // vlmNr, vlmPojo
                    boolean restorable = true;

                    for (Entry<Integer, BackupInfoPojo> entry: s3MetaVlmMap.entrySet())
                    {
                        Integer s3MetaVlmNr = entry.getKey();
                        BackupInfoPojo s3BackVlmInfo  = entry.getValue();
                        if (!s3keys.contains(s3BackVlmInfo.getName()))
                        {
                            /*
                             * The metafile is referring to a data-file that is not known in the given bucket
                             */
                            restorable = false;
                        }
                        else
                        {
                            Matcher s3BackupKeyMatcher = BACKUP_KEY_PATTERN.matcher(s3BackVlmInfo.getName());
                            if (s3BackupKeyMatcher.matches())
                            {
                                Integer s3VlmNrFromBackupName = Integer.parseInt(s3BackupKeyMatcher.group(3));
                                if (s3MetaVlmNr == s3VlmNrFromBackupName)
                                {
                                    long vlmFinishedTime = s3BackVlmInfo.getFinishedTimestamp();

                                    BackupVolumePojo retVlmPojo = new BackupVolumePojo(
                                        s3MetaVlmNr,
                                        BackupApi.DATE_FORMAT.format(new Date(vlmFinishedTime)),
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
                    // get rid of ".meta"
                    String id = s3key.substring(0, s3key.length() - 5);
                    String basedOn = s3MetaFile.getBasedOn();
                    BackupApi back = new BackupPojo(
                        id,
                        metaFileMatcher.group(1),
                        metaFileMatcher.group(2),
                        BackupApi.DATE_FORMAT.format(new Date(s3MetaFile.getStartTimestamp())),
                        s3MetaFile.getStartTimestamp(),
                        BackupApi.DATE_FORMAT.format(new Date(s3MetaFile.getFinishTimestamp())),
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
                Matcher m = BACKUP_KEY_PATTERN.matcher(s3key);
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
                        BACKUP_KEY_PATTERN,
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

                StateFlags<Flags> snapDfnFlags = snapDfn.getFlags();
                if (snapDfnFlags.isSet(peerCtx, SnapshotDefinition.Flags.BACKUP) &&
                    // ignore already shipped backups
                    !snapDfnFlags.isSet(peerCtx, SnapshotDefinition.Flags.SUCCESSFUL)
                )
                {
                    Set<String> futureS3Keys = new TreeSet<>();
                    for (SnapshotVolumeDefinition snapVlmDfn : snapDfn.getAllSnapshotVolumeDefinitions(peerCtx))
                    {
                        futureS3Keys.add(
                            String.format(
                                BackupShippingConsts.BACKUP_KEY_FORMAT,
                                rscName,
                                "",
                                snapVlmDfn.getVolumeNumber().value,
                                snapName
                            )
                        );
                    }

                    String s3KeyShouldLookLikeThis = futureS3Keys.iterator().next();
                    Matcher m = BACKUP_KEY_PATTERN.matcher(s3KeyShouldLookLikeThis);
                    if (m.find())
                    {
                        BackupApi back = fillBackupListPojo(
                            s3KeyShouldLookLikeThis,
                            rscName,
                            snapName,
                            m,
                            BACKUP_KEY_PATTERN,
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
        {}
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

        try {
            String startTime = snapName.substring(BackupShippingConsts.SNAP_PREFIX_LEN);
            long startTimestamp = BackupApi.DATE_FORMAT.parse(startTime).getTime(); // fail fast

            Map<Integer, BackupVolumePojo> vlms = new TreeMap<>();
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
            Boolean shipping;
            Boolean success;
            String nodeName = null;
            try
            {
                AccessContext peerCtx = peerAccCtx.get();
                if (snapDfn != null && snapDfn.getFlags().isSet(peerCtx, SnapshotDefinition.Flags.BACKUP))
                {
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
        }
        catch (ParseException exc)
        {
            errorReporter.reportError(exc, peerAccCtx.get(), null, "used s3 key: " + s3key);
        }
        return back;
    }

    public Flux<ApiCallRc> backupAbort(String rscNameRef, boolean restore, boolean create) {
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
        return updateStlts.transform(
                responses -> CtrlResponseUtils.combineResponses(
                    responses,
                    LinstorParsingUtils.asRscName(rscNameRef),
                "Abort backups of {1} on {0} started"
                )
            );
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

            Snapshot nextSnap = backupInfoMgr.getNextBackupToUpload(snap);
            if (successRef && nextSnap != null)
            {
                SnapshotDefinition nextSnapDfn = nextSnap.getSnapshotDefinition();
                nextSnapDfn.getFlags().enableFlags(peerCtx, SnapshotDefinition.Flags.SHIPPING);
                nextSnap.getFlags().enableFlags(peerCtx, Snapshot.Flags.BACKUP_TARGET);

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
            }
            else
            {
                backupInfoMgr.restoreRemoveEntry(snapDfn.getResourceDefinition());
                if (successRef)
                {
                    // start snap-restore
                    flux = ctrlSnapRestoreApiCallHandler.restoreSnapshotFromBackup(
                        Collections.emptyList(),
                        snapNameRef,
                        rscNameRef
                    );
                }
                else
                {
                    flux = ctrlSnapDeleteApiCallHandler.deleteSnapshot(
                        snapDfn.getResourceName().displayValue, snapDfn.getName().displayValue
                    );
                }
            }
            ctrlTransactionHelper.commit();
            return flux;
        }
        catch (AccessDeniedException | InvalidKeyException exc)
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
            NodeName nodeName = peerProvider.get().getNode().getName();
            backupInfoMgr.abortDeleteEntries(nodeName.displayValue, rscNameRef, snapNameRef);
            if (!successRef && snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING_ABORT))
            {
                // re-enable shipping-flag to make sure the abort-logic gets triggered later on
                snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
                ctrlTransactionHelper.commit();
                return ctrlSnapShipAbortHandler.abortBackupShippingPrivileged(snapDfn.getResourceDefinition());
            }
            snapDfn.getFlags().disableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
            if (successRef)
            {
                snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED);
            }
            String remoteName = "";
            Snapshot snap = snapDfn.getSnapshot(peerAccCtx.get(), nodeName);
            remoteName = snap.getProps(peerAccCtx.get())
                .removeProp(InternalApiConsts.KEY_BACKUP_TARGET_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING);
            snap.getProps(peerAccCtx.get())
                .removeProp(InternalApiConsts.KEY_BACKUP_NODE_IDS_TO_RESET, ApiConsts.NAMESPC_BACKUP_SHIPPING);
            if (successRef)
            {
                ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
                rscDfn.getProps(peerAccCtx.get()).setProp(
                    InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                    snapNameRef,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" + remoteName
                );
            }
            ctrlTransactionHelper.commit();

            return ctrlSatelliteUpdateCaller.updateSatellites(
                snapDfn,
                CtrlSatelliteUpdateCaller.notConnectedWarn()
            ).transform(
                responses -> CtrlResponseUtils.combineResponses(
                    responses,
                    LinstorParsingUtils.asRscName(rscNameRef),
                    "Finishing shipping of backup ''" + snapNameRef + "'' of {1} on {0}"
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
            errors.addEntry(getErrorRcIfNotSupported(deviceProviderKind, extToolsManager, ExtTools.ZSTD, "zstd", null));
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

    private String buildMetaName(String rscName, String snapName)
    {
        return rscName + "_" + snapName + ".meta";
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
