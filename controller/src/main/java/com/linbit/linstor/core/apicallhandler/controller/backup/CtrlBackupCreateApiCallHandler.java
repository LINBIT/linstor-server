package com.linbit.linstor.core.apicallhandler.controller.backup;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotShippingAbortHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupApiHelper.S3ObjectInfo;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
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
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
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
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import reactor.core.publisher.Flux;

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
        CtrlScheduledBackupsApiCallHandler scheduledBackupsHandlerRef
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
                RemoteType.S3,
                scheduleNameRef,
                runInBackgroundRef
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
        boolean setShippingFlag,
        boolean allowIncremental,
        Map<ExtTools, ExtToolsInfo.Version> requiredExtTools,
        Map<ExtTools, ExtToolsInfo.Version> optionalExtTools,
        RemoteType remoteTypeRef,
        String scheduleNameRef,
        boolean runInBackgroundRef
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
            RemoteName linstorRemoteName = null;
            SnapshotDefinition prevSnapDfn = null;
            if (remoteTypeRef.equals(RemoteType.S3))
            {
                // check if encryption is possible
                backupHelper.getLocalMasterKey();

                // check if remote exists (only needed for s3, other option here is stlt, which is created right before
                // this method is called)
                remote = backupHelper.getRemote(remoteName);
                prevSnapDfn = getIncrementalBase(rscDfn, remoteName, remote, allowIncremental);
            }
            else if (remoteTypeRef.equals(RemoteType.SATELLITE))
            {
                remote = backupHelper.getRemote(remoteName);
                if (remote instanceof StltRemote)
                {
                    linstorRemoteName = ((StltRemote) remote).getLinstorRemoteName();
                    prevSnapDfn = getIncrementalBase(rscDfn, linstorRemoteName.displayValue, remote, allowIncremental);
                }
            }

            Collection<SnapshotDefinition> snapDfns = backupHelper.getInProgressBackups(rscDfn, remote);
            if (!snapDfns.isEmpty())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_EXISTS_SNAPSHOT_SHIPPING,
                        "Backup shipping of resource '" + rscNameRef + "' already in progress"
                    )
                );
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

            SnapshotDefinition snapDfn = snapCrtHelper
                .createSnapshots(nodes, rscDfn.getName().displayValue, snapName, responses);
            setBackupSnapDfnFlagsAndProps(snapDfn, scheduleNameRef, nowRef);

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
            setStartBackupProps(createdSnapshot, snapDfn, remoteName, linstorRemoteName, nodeIds);

            if (linstorRemoteName == null)
            {
                // we do not have a linstorRemoteName, so this is not a stltRemote
                setIncrementalDependentProps(createdSnapshot, prevSnapDfn, remoteName, scheduleNameRef);
            }

            ctrlTransactionHelper.commit();

            responses.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.MASK_INFO, "Shipping of resource " + rscNameRef + " to remote " + remoteName +
                        " in progress."
                ).putObjRef(ApiConsts.KEY_SNAPSHOT, snapName).build()
            );

            Flux<ApiCallRc> flux = snapshotCrtHandler.postCreateSnapshot(snapDfn, runInBackgroundRef)
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
        catch (InvalidKeyException | InvalidNameException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void setBackupSnapDfnFlagsAndProps(SnapshotDefinition snapDfn, String scheduleNameRef, Date nowRef)
        throws AccessDeniedException, DatabaseException, InvalidKeyException, InvalidValueException
    {
        snapDfn.getFlags()
            .enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING, SnapshotDefinition.Flags.BACKUP);

        snapDfn.getProps(peerAccCtx.get()).setProp(
            InternalApiConsts.KEY_BACKUP_START_TIMESTAMP,
            Long.toString(nowRef.getTime()),
            ApiConsts.NAMESPC_BACKUP_SHIPPING
        );
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
        Snapshot snap,
        SnapshotDefinition snapDfn,
        String remoteName,
        RemoteName linstorRemoteName,
        List<Integer> nodeIds
    ) throws InvalidKeyException, AccessDeniedException, DatabaseException, InvalidValueException
    {
        Props snapProps = snap.getProps(peerAccCtx.get());
        snapDfn.getProps(peerAccCtx.get()).setProp(
            InternalApiConsts.KEY_BACKUP_NODE_IDS_TO_RESET,
            StringUtils.join(nodeIds, InternalApiConsts.KEY_BACKUP_NODE_ID_SEPERATOR),
            ApiConsts.NAMESPC_BACKUP_SHIPPING
        );
        snapProps.setProp(
            InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
            remoteName,
            ApiConsts.NAMESPC_BACKUP_SHIPPING
        );
        // also needed on snapDfn only for scheduled shippings
        if (linstorRemoteName != null)
        {
            snapDfn.getProps(peerAccCtx.get()).setProp(
                InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                linstorRemoteName.getDisplayName(),
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
        }
        else
        {
            snapDfn.getProps(peerAccCtx.get()).setProp(
                InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                remoteName,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
        }
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
        String remoteName,
        AbsRemote remote,
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
                        "Could not create an incremental backup for resource %s as the previous snapshot %s needed " +
                            "for the incremental backup has already been deleted. Creating a full backup instead.",
                        rscDfn.getName(),
                        prevSnapName
                    );
                }
                else
                {
                    if (remote instanceof S3Remote)
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
            boolean isSomeSortOfDiskless = rsc.getStateFlags().isSomeSet(
                peerAccCtx.get(),
                Resource.Flags.DRBD_DISKLESS,
                Resource.Flags.NVME_INITIATOR,
                Resource.Flags.EBS_INITIATOR
            );
            if (!isSomeSortOfDiskless)
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
        if (nodeNamesStr.isEmpty())
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

    /**
     * Makes sure the given node has all ext-tools given
     */
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
        Snapshot snap,
        SnapshotDefinition prevSnapDfn,
        String remoteName,
        String scheduleName
    )
        throws InvalidValueException, AccessDeniedException, DatabaseException
    {
        SnapshotDefinition snapDfn = snap.getSnapshotDefinition();
        Props snapDfnProps = snapDfn.getProps(peerAccCtx.get());
        Props rscDfnProps = snapDfn.getResourceDefinition().getProps(peerAccCtx.get());
        if (prevSnapDfn == null)
        {
            snapDfnProps.setProp(
                InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP,
                snap.getSnapshotName().displayValue,
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
            snap.getProps(peerAccCtx.get()).setProp(
                InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT,
                prevSnapDfn.getName().displayValue,
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
                backupInfoMgr.abortCreateDeleteEntries(nodeName.displayValue, rscNameRef, snapNameRef);
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
                        Pair<Flux<ApiCallRc>, AbsRemote> pair = getRemoteForScheduleAndCleanupFlux(
                            remote,
                            rscDfn,
                            snapNameRef,
                            successRef
                        );

                        remoteForSchedule = pair.objB;
                        cleanupFlux = cleanupFlux.concatWith(pair.objA);
                    }
                    ctrlTransactionHelper.commit();
                    Flux<ApiCallRc> flux = ctrlSatelliteUpdateCaller.updateSatellites(
                        snapDfn,
                        CtrlSatelliteUpdateCaller.notConnectedWarn()
                    ).transform(
                        responses -> CtrlResponseUtils.combineResponses(
                            responses,
                            LinstorParsingUtils.asRscName(rscNameRef),
                            "Finishing shipping of backup ''" + snapNameRef + "'' of {1} on {0}"
                        )
                            .concatWith(
                                backupHelper.startStltCleanup(
                                    peerProvider.get(), rscNameRef, snapNameRef, peerProvider.get().getNode().getName()
                                )
                            )
                    );
                    // cleanupFlux will not be executed if flux has an error - this issue is currently unavoidable.
                    // This will be fixed with the linstor2 issue 19 (Combine Changed* proto messages for atomic
                    // updates)
                    cleanupFlux = flux.concatWith(cleanupFlux);
                }

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

    /**
     * Checks if the given rsc has the correct device-provider and ext-tools to be shipped as a backup
     */
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

    /**
     * Checks if the given ext-tool is supported and returns an error-rc instead of throwing an exception if not.
     */
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
        else if (version != null && !info.hasVersionOrHigher(version))
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
}
