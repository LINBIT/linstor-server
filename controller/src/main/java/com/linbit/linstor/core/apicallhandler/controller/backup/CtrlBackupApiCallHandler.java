package com.linbit.linstor.core.apicallhandler.controller.backup;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.BackupToS3;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.backups.BackupInfoPojo;
import com.linbit.linstor.api.pojo.backups.BackupInfoStorPoolPojo;
import com.linbit.linstor.api.pojo.backups.BackupInfoVlmPojo;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.api.pojo.backups.BackupMetaInfoPojo;
import com.linbit.linstor.api.pojo.backups.BackupPojo;
import com.linbit.linstor.api.pojo.backups.BackupPojo.BackupS3Pojo;
import com.linbit.linstor.api.pojo.backups.BackupPojo.BackupVlmS3Pojo;
import com.linbit.linstor.api.pojo.backups.BackupPojo.BackupVolumePojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.backupshipping.BackupConsts;
import com.linbit.linstor.backupshipping.S3MetafileNameInfo;
import com.linbit.linstor.backupshipping.S3VolumeNameInfo;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupApiHelper.S3ObjectInfo;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingPrepareAbortRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingRestClient;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apis.BackupApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfProtectionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;
import com.linbit.utils.TimeUtils;

import static com.linbit.linstor.backupshipping.BackupConsts.META_SUFFIX;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ErrorReporter errorReporter;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final BackupToS3 backupHandler;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteApiCallHandler;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final BackupInfoManager backupInfoMgr;
    private final SystemConfProtectionRepository sysCfgRepo;
    private final ResourceDefinitionRepository rscDfnRepo;
    private final CtrlBackupApiHelper backupHelper;
    private final BackupShippingRestClient backupClient;

    @Inject
    public CtrlBackupApiCallHandler(
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @SystemContext AccessContext sysCtxRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ErrorReporter errorReporterRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        BackupToS3 backupHandlerRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteApiCallHandlerRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        BackupInfoManager backupInfoMgrRef,
        SystemConfProtectionRepository sysCfgRepoRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        CtrlBackupApiHelper backupHelperRef,
        BackupShippingRestClient backupClientRef
    )
    {
        peerAccCtx = peerAccCtxRef;
        sysCtx = sysCtxRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        errorReporter = errorReporterRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        backupHandler = backupHandlerRef;
        ctrlSnapDeleteApiCallHandler = ctrlSnapDeleteApiCallHandlerRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        backupInfoMgr = backupInfoMgrRef;
        sysCfgRepo = sysCfgRepoRef;
        rscDfnRepo = rscDfnRepoRef;
        backupHelper = backupHelperRef;
        backupClient = backupClientRef;
    }

    public Flux<ApiCallRc> deleteBackup(
        String rscName,
        String id,
        @Nullable String idPrefix,
        @Nullable String timestamp,
        @Nullable String nodeName,
        boolean cascading,
        boolean allLocalCluster,
        boolean all,
        @Nullable String s3Key,
        String remoteName,
        boolean dryRun,
        boolean keepSnaps
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
                dryRun,
                keepSnaps
            )
        );
    }

    /**
     * Delete backups from an s3-remote</br>
     * The following combinations are allowed:
     * <dl>
     * <dt>id [cascading]</dt>
     * <dd>delete the backup with this exact name</dd>
     * <dt>idPrefix [cascading]</dt>
     * <dd>delete all backups starting with this name</dd>
     * <dt>s3Key [cascading]</dt>
     * <dd>delete this exact s3-object, whether it is part of a backup or not</dd>
     * <dt>(timestamp|rscName|nodeName)+ [cascading]</dt>
     * <dd>delete all backups fitting the filter:
     * <ul style="list-style: none; margin-bottom: 0px;">
     * <li>timestamp: created before the timestamp</li>
     * <li>rscName: created from this rsc</li>
     * <li>nodeName: uploaded from this node</li>
     * </ul>
     * </dd>
     * <dt>all // force cascading</dt>
     * <dd>delete all backups on the given remote</dd>
     * <dt>allLocalCluster // forced cascading</dt>
     * <dd>delete all backups on the given remote that originated from this cluster</dd>
     * </dl>
     * additionally, all combinations can have these set:
     * <dl>
     * <dt>dryRun</dt>
     * <dd>only prints out what would be deleted</dd>
     * <dt>keepSnaps</dt>
     * <dd>only deletes the backups, no local snapshots</dd>
     * </dl>
     */
    private Flux<ApiCallRc> deleteBackupInTransaction(
        String id,
        String idPrefix,
        boolean cascading,
        @Nullable String rscName,
        @Nullable String nodeName,
        @Nullable String timestamp,
        boolean allLocalCluster,
        boolean all,
        @Nullable String s3Key,
        String remoteName,
        boolean dryRun,
        boolean keepSnaps
    ) throws AccessDeniedException, InvalidNameException
    {
        S3Remote s3Remote = backupHelper.getS3Remote(remoteName);
        ToDeleteCollections toDelete = new ToDeleteCollections();

        Map<String, S3ObjectInfo> s3LinstorObjects = backupHelper.loadAllLinstorS3Objects(
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
        if (keepSnaps)
        {
            toDelete.snapKeys.clear();
        }

        Flux<ApiCallRc> flux = null;
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
                        snapKey.getResourceName(),
                        snapKey.getSnapshotName(),
                        null
                    )
                );
            }
            try
            {
                if (!toDelete.s3keys.isEmpty())
                {
                    backupHandler
                        .deleteObjects(toDelete.s3keys, s3Remote, peerAccCtx.get(), backupHelper.getLocalMasterKey());
                }
                else
                {
                    apiCallRc.addEntry(
                        "Could not find any backups to delete.",
                        ApiConsts.INFO_NOOP | ApiConsts.MASK_BACKUP
                    );
                    flux = Flux.just(apiCallRc);
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
        if (flux == null)
        {
            flux = Flux.<ApiCallRc>just(apiCallRc).concatWith(deleteSnapFlux);
        }
        return flux;
    }

    /**
     * Finds all keys that match the given idPrefix. If allowMultiSelection is false, makes sure that there is only
     * one match. Afterwards, calls deleteByS3Key with the key(s) that were a match.
     */
    private void deleteByIdPrefix(
        String idPrefixRef,
        boolean allowMultiSelectionRef,
        boolean cascadingRef,
        Map<String, S3ObjectInfo> s3LinstorObjects,
        S3Remote s3RemoteRef,
        ToDeleteCollections toDeleteRef
    )
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

    /**
     * Checks whether the given s3KeysToDelete exist, then calls addToDeleteList for those that do.
     */
    private void deleteByS3Key(
        Map<String, S3ObjectInfo> s3LinstorObjects,
        Set<String> s3KeysToDeleteRef,
        boolean cascadingRef,
        ToDeleteCollections toDeleteRef
    )
    {
        for (String s3Key : s3KeysToDeleteRef)
        {
            S3ObjectInfo s3ObjectInfo = s3LinstorObjects.get(s3Key);
            if (s3ObjectInfo != null && s3ObjectInfo.doesExist())
            {
                addToDeleteList(s3LinstorObjects, s3ObjectInfo, cascadingRef, toDeleteRef);
            }
            else
            {
                toDeleteRef.s3KeysNotFound.add(s3Key);
            }
        }
    }

    /**
     * Make sure all child-objects get marked for deletion as well, and throw an error if there are child-objects
     * but cascading is false. Also mark all related snapDfns for deletion.
     */
    private static void addToDeleteList(
        Map<String, S3ObjectInfo> s3Map,
        S3ObjectInfo s3ObjectInfo,
        boolean cascading,
        ToDeleteCollections toDelete
    )
    {
        if (s3ObjectInfo.isMetaFile())
        {
            toDelete.s3keys.add(s3ObjectInfo.getS3Key());
            for (S3ObjectInfo childObj : s3ObjectInfo.getReferences())
            {
                if (childObj.doesExist())
                {
                    if (!childObj.isMetaFile())
                    {
                        toDelete.s3keys.add(childObj.getS3Key());
                    }
                    // we do not want to cascade upwards. only delete child / data keys
                }
                else
                {
                    toDelete.s3KeysNotFound.add(childObj.getS3Key());
                }
            }
            for (S3ObjectInfo childObj : s3ObjectInfo.getReferencedBy())
            {
                if (childObj.doesExist())
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
                                    s3ObjectInfo.getS3Key() + " should be deleted, but at least " +
                                        childObj.getS3Key() +
                                        " is referencing it. Use --cascading to delete recursively"
                                )
                            );
                        }
                    }
                    // we should not be referenced by something other than a metaFile
                }
                else
                {
                    toDelete.s3KeysNotFound.add(childObj.getS3Key());
                }
            }
            SnapshotDefinition snapDfn = s3ObjectInfo.getSnapDfn();
            if (snapDfn != null)
            {
                toDelete.snapKeys.add(snapDfn.getSnapDfnKey());
            }
        }
    }

    /**
     * Find all meta-files that conform to the given filters (timestamp, rscName, nodeName), then call
     * deleteByS3Key with that list.
     */
    private void deleteByTimeRscNode(
        Map<String, S3ObjectInfo> s3LinstorObjectsRef,
        String timestampRef,
        String rscNameRef,
        String nodeNameRef,
        boolean cascadingRef,
        ToDeleteCollections toDeleteRef
    )
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
                LocalDateTime date = BackupConsts.DATE_FORMAT.parse(timestampRef, LocalDateTime::from);
                timestampCheck = timestamp -> date.isAfter(TimeUtils.millisToDate(timestamp));
            }
            catch (DateTimeParseException exc)
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
                String s3Key = s3Obj.getS3Key();
                BackupMetaDataPojo metaFile = s3Obj.getMetaFile();
                String node = metaFile.getNodeName();
                String rsc = metaFile.getRscName();
                long startTimestamp;
                try
                {
                    S3MetafileNameInfo meta = new S3MetafileNameInfo(s3Key);
                    startTimestamp = TimeUtils.getEpochMillis(meta.backupTime);
                }
                catch (ParseException exc)
                {
                    throw new ImplementationError("Invalid meta file name", exc);
                }
                if (nodeNameCheck.test(node) && rscNameCheck.test(rsc) && timestampCheck.test(startTimestamp))
                {
                    s3KeysToDelete.add(s3Key);
                }
            }
        }
        deleteByS3Key(s3LinstorObjectsRef, s3KeysToDelete, cascadingRef, toDeleteRef);
    }

    /**
     * Find all meta-files that were created by the local cluster, then call deleteByS3Key with that list.
     */
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
            BackupMetaDataPojo metaFile = s3Obj.getMetaFile();
            if (metaFile != null && localClusterId.equals(metaFile.getClusterId()))
            {
                s3KeysToDelete.add(s3Obj.getS3Key());
            }
        }
        deleteByS3Key(s3LinstorObjectsRef, s3KeysToDelete, true, toDeleteRef);
    }

    /**
     * @return
     * <code>Pair.objA</code>: Map of s3Key -> backupApi <br />
     * <code>Pair.objB</code>: Set of s3Keys that either were not created by linstor or cannot be recognized as such
     * anymore
     */
    public Pair<Map<String, BackupApi>, Set<String>> listBackups(
        String rscNameRef,
        String snapNameRef,
        String remoteNameRef
    )
        throws AccessDeniedException, InvalidNameException
    {
        S3Remote remote = backupHelper.getS3Remote(remoteNameRef);
        AccessContext peerCtx = peerAccCtx.get();

        // get ALL s3 keys of the given bucket, including possibly not linstor related ones
        Set<String> s3keys = backupHelper.getAllS3Keys(remote, rscNameRef);

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
                if (snapNameRef != null && !snapNameRef.isEmpty() && !snapNameRef.equalsIgnoreCase(info.snapName))
                {
                    // Doesn't match the requested snapshot name, skip it.
                    continue;
                }
                PairNonNull<BackupApi, Set<String>> result = getBackupFromMetadata(
                    peerCtx,
                    s3key,
                    info,
                    remote,
                    s3keys
                );
                BackupApi back = result.objA;
                retIdToBackupsApiMap.put(back.getId(), back);
                linstorBackupsS3Keys.add(s3key);
                linstorBackupsS3Keys.addAll(result.objB);
                String base = back.getBasedOnId();
                if (base != null && !base.isEmpty())
                {
                    List<BackupApi> usedByList = idToUsedByBackupApiMap
                        .computeIfAbsent(base, s -> new ArrayList<>());
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
                    if (snapNameRef != null && !snapNameRef.isEmpty() && !snapNameRef.equalsIgnoreCase(info.snapName))
                    {
                        // Doesn't match the requested snapshot name, skip it.
                        continue;
                    }
                    SnapshotDefinition snapDfn = backupHelper.loadSnapDfnIfExists(info.rscName, info.snapName);

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
        // also check local snapDfns if anything is being uploaded but not yet visible in the s3 list (an upload might
        // only be shown in the list when it is completed)
        for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView(peerCtx).values())
        {
            if (
                rscNameRef != null && !rscNameRef.isEmpty() &&
                    rscDfn.getName().displayValue.equalsIgnoreCase(rscNameRef)
            )
            {
                // Doesn't match the given rsc name, skip it.
                continue;
            }
            // only check in-progress snapDfns
            for (SnapshotDefinition snapDfn : backupHelper.getInProgressBackups(rscDfn))
            {
                String rscName = rscDfn.getName().displayValue;
                String snapName = snapDfn.getName().displayValue;

                if (snapNameRef != null && !snapNameRef.isEmpty() && snapNameRef.equalsIgnoreCase(snapName))
                {
                    // Doesn't match the requested snapshot name, skip it.
                    continue;
                }

                String s3Suffix = snapDfn.getSnapDfnProps(peerCtx)
                    .getProp(
                    ApiConsts.KEY_BACKUP_S3_SUFFIX,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                );

                String backupTimeRaw = snapDfn.getSnapDfnProps(peerCtx)
                    .getProp(InternalApiConsts.KEY_BACKUP_START_TIMESTAMP, ApiConsts.NAMESPC_BACKUP_SHIPPING);

                LocalDateTime backupTime = TimeUtils.millisToDate(Long.parseLong(backupTimeRaw));

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

        s3keys.removeAll(linstorBackupsS3Keys);
        return new Pair<>(retIdToBackupsApiMap, s3keys);
    }

    /**
     * Get all information needed for listBackups from the meta-file
     */
    private PairNonNull<BackupApi, Set<String>> getBackupFromMetadata(
        AccessContext peerCtx,
        String metadataKey,
        S3MetafileNameInfo info,
        S3Remote remote,
        Set<String> allS3keys
    )
        throws IOException, AccessDeniedException
    {
        BackupMetaDataPojo s3MetaFile = backupHandler
            .getMetaFile(metadataKey, remote, peerCtx, backupHelper.getLocalMasterKey());

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
                                BackupConsts.DATE_FORMAT.format(TimeUtils.millisToDate(vlmFinishedTime)),
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

        return new PairNonNull<>(
            new BackupPojo(
                id,
                info.rscName,
                info.snapName,
                BackupConsts.DATE_FORMAT.format(TimeUtils.millisToDate(s3MetaFile.getStartTimestamp())),
                s3MetaFile.getStartTimestamp(),
                BackupConsts.DATE_FORMAT.format(TimeUtils.millisToDate(s3MetaFile.getFinishTimestamp())),
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
     * Get all information possible for listBackups from a volume backup that is missing its meta-file
     */
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
                String ts = snapDfn.getSnapDfnProps(peerCtx)
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
                    else // if isShipped
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
            BackupConsts.DATE_FORMAT.format(info.backupTime),
            TimeUtils.getEpochMillis(info.backupTime),
            null,
            null,
            nodeName,
            shipping,
            success,
            false,
            vlms,
            null,
            null
        );
    }

    public Flux<ApiCallRc> backupAbort(String rscNameRef, boolean restore, boolean create, String remoteNameRef)
    {
        return scopeRunner.fluxInTransactionalScope(
            "prepare for abort backup",
            lockGuardFactory.create().read(LockObj.NODES_MAP).write(LockObj.RSC_DFN_MAP).buildDeferred(),
            () -> backupAbortInTransaction(rscNameRef, restore, create, remoteNameRef)
        );
    }

    /**
     * Check if create or restore needs to be aborted if not specified by the parameters, then set SHIPPING_ABORT on all
     * affected snapDfns
     */
    private Flux<ApiCallRc> backupAbortInTransaction(
        String rscNameRef,
        boolean restorePrm,
        boolean createPrm,
        String remoteNameRef
    )
        throws AccessDeniedException, DatabaseException, InvalidNameException
    {
        Flux<ApiCallRc> flux = Flux.empty();
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
        AbsRemote remote = ctrlApiDataLoader.loadRemote(remoteNameRef, true);
        // immediately remove any queued snapshots
        for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(peerAccCtx.get()))
        {
            backupInfoMgr.deleteFromQueue(snapDfn, remote);
        }
        Set<SnapshotDefinition> snapDfns = backupHelper.getInProgressBackups(rscDfn);
        if (!snapDfns.isEmpty())
        {
            boolean restore = restorePrm;
            boolean create = createPrm;
            if (!restore && !create)
            {
                restore = true;
                create = true;
            }

            Set<SnapshotDefinition> snapDfnsToUpdateOnlyShipping = new HashSet<>();
            Set<SnapshotDefinition> snapDfnsToUpdateShippingAbort = new HashSet<>();
            List<PairNonNull<String, String>> stltRemoteAndSnapNamesToUpdateShippingAbort = new ArrayList<>();
            for (SnapshotDefinition snapDfn : snapDfns)
            {
                Collection<Snapshot> snaps = snapDfn.getAllSnapshots(peerAccCtx.get());
                boolean abort = false;
                boolean isSnapDfnSource = false;
                boolean isSnapDfnTarget = false;
                for (Snapshot snap : snaps)
                {
                    boolean isSnapSource = snap.getFlags().isSet(peerAccCtx.get(), Snapshot.Flags.BACKUP_SOURCE);
                    isSnapDfnSource |= isSnapSource;
                    boolean crt = isSnapSource && create;
                    boolean isSnapTarget = snap.getFlags().isSet(peerAccCtx.get(), Snapshot.Flags.BACKUP_TARGET);
                    isSnapDfnTarget |= isSnapTarget;
                    boolean rst = isSnapTarget && restore;
                    if (crt)
                    {
                        String remoteName = snap.getSnapProps(peerAccCtx.get())
                            .getProp(InternalApiConsts.KEY_BACKUP_TARGET_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                        crt = backupHelper.hasShippingToRemote(remoteName, remoteNameRef);
                        if (crt)
                        {
                            // this has to be the stlt-remote-name if we are in an l2l-abort, if we are in an s3-abort
                            // this set will not be used anyway, so no need to check for that
                            stltRemoteAndSnapNamesToUpdateShippingAbort.add(
                                new PairNonNull<>(remoteName, snap.getSnapshotName().displayValue)
                            );
                        }
                    }
                    if (rst)
                    {
                        String remoteName = snap.getSnapProps(peerAccCtx.get())
                            .getProp(InternalApiConsts.KEY_BACKUP_SRC_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                        rst = backupHelper.hasShippingToRemote(remoteName, remoteNameRef);
                        if (rst)
                        {
                            // this has to be the stlt-remote-name if we are in an l2l-abort, if we are in an s3-abort
                            // this set will not be used anyway, so no need to check for that
                            stltRemoteAndSnapNamesToUpdateShippingAbort.add(
                                new PairNonNull<>(remoteName, snap.getSnapshotName().displayValue)
                            );
                        }
                    }
                    if (crt || rst)
                    {
                        abort = true;
                        // there should always be only one snapshot of any given snapDfn that is actually shipping, so
                        // once we found that one, we can stop looking at any others
                        break;
                    }
                }
                if (!isSnapDfnSource && create && !isSnapDfnTarget)
                {
                    // this can happen for l2l-shipments if the target-cluster fails to start the receive, since
                    // BACKUP_SOURCE is set in a later transaction than SHIPPING, therefore we need to remove the
                    // SHIPPING flag if we are aborting creates (otherwise the snap is stuck unable to be aborted or
                    // deleted)
                    snapDfnsToUpdateOnlyShipping.add(snapDfn);
                }
                if (abort)
                {
                    snapDfnsToUpdateShippingAbort.add(snapDfn);
                }
            }
            if (!snapDfnsToUpdateShippingAbort.isEmpty() && !(remote instanceof S3Remote))
            {
                if (remote instanceof StltRemote)
                {
                    // it should not be possible for this remote to be a stltRemote, but just in case someone didn't pay
                    // attention while calling this method...
                    remote = ctrlApiDataLoader.loadRemote(((StltRemote) remote).getLinstorRemoteName(), true);
                }
                String localClusterId;
                try
                {
                    localClusterId = sysCfgRepo.getCtrlConfForView(sysCtx)
                        .getProp(
                            InternalApiConsts.KEY_CLUSTER_LOCAL_ID,
                            ApiConsts.NAMESPC_CLUSTER
                        );
                }
                catch (InvalidKeyException | AccessDeniedException exc)
                {
                    throw new ImplementationError(exc);
                }
                // get stltRemote from prop - if prop not set, we should be src and early, and target does not need to
                // prepare abort
                BackupShippingPrepareAbortRequest data = new BackupShippingPrepareAbortRequest(
                    new ApiCallRcImpl(),
                    localClusterId,
                    getOtherRscNamesFromStltRemotes(stltRemoteAndSnapNamesToUpdateShippingAbort),
                    LinStor.VERSION_INFO_PROVIDER.getSemanticVersion()
                );
                LinstorRemote l2lRemote = (LinstorRemote) remote;
                flux = backupClient.sendPrepareAbortRequest(data, l2lRemote, peerAccCtx.get())
                    .map(Json::jsonToApiCallRc);
            }
            flux = flux.concatWith(
                setFlagsAndAbort(
                    snapDfnsToUpdateOnlyShipping,
                    snapDfnsToUpdateShippingAbort,
                    rscNameRef,
                    create,
                    restore
                )
            );
        }
        return flux;
    }

    private Map<String, List<String>> getOtherRscNamesFromStltRemotes(
        List<PairNonNull<String, String>> remoteSnapPairList
    )
        throws InvalidNameException
    {
        Map<String, List<String>> ret = new HashMap<>();
        for (PairNonNull<String, String> remoteSnapPair : remoteSnapPairList)
        {
            StltRemote remote = (StltRemote) ctrlApiDataLoader.loadRemote(
                new RemoteName(remoteSnapPair.objA, true),
                true
            );
            ret.computeIfAbsent(remote.getOtherRscName(), ignored -> new ArrayList<>())
                .add(remoteSnapPair.objB);
        }
        return ret;
    }

    private Flux<ApiCallRc> setFlagsAndAbort(
        Set<SnapshotDefinition> snapDfnsToUpdateOnlyShipping,
        Set<SnapshotDefinition> snapDfnsToUpdateShippingAbort,
        String rscName,
        boolean create,
        boolean restore
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "set flags and abort backup",
            lockGuardFactory.create().read(LockObj.NODES_MAP).write(LockObj.RSC_DFN_MAP).buildDeferred(),
            () -> setFlagsAndAbortInTransaction(
                snapDfnsToUpdateOnlyShipping,
                snapDfnsToUpdateShippingAbort,
                rscName,
                create,
                restore
            )
        );
    }

    private Flux<ApiCallRc> setFlagsAndAbortInTransaction(
        Set<SnapshotDefinition> snapDfnsToUpdateOnlyShipping,
        Set<SnapshotDefinition> snapDfnsToUpdateShippingAbort,
        String rscName,
        boolean create,
        boolean restore
    ) throws AccessDeniedException, DatabaseException
    {
        for (SnapshotDefinition snapDfn : snapDfnsToUpdateOnlyShipping)
        {
            snapDfn.getFlags().disableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
        }
        for (SnapshotDefinition snapDfn : snapDfnsToUpdateShippingAbort)
        {
            snapDfn.getFlags().disableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
            snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING_ABORT);
        }
        ctrlTransactionHelper.commit();

        Set<SnapshotDefinition> snapDfnsToUpdate = new HashSet<>(snapDfnsToUpdateOnlyShipping);
        snapDfnsToUpdate.addAll(snapDfnsToUpdateShippingAbort);
        Flux<Tuple2<NodeName, Flux<ApiCallRc>>> flux = Flux.empty();
        for (SnapshotDefinition snapDfn : snapDfnsToUpdate)
        {
            flux = flux.concatWith(
                ctrlSatelliteUpdateCaller.updateSatellites(snapDfn, CtrlSatelliteUpdateCaller.notConnectedWarn())
            );
        }
        ApiCallRcImpl success = new ApiCallRcImpl();
        success.addEntry(
            "Successfully aborted all " +
                ((create && restore) ?
                    "in-progress backup-shipments and restores" :
                    (create ? "in-progress backup-shipments" : "in-progress backup-restores")) +
                " of resource " + rscName,
            ApiConsts.MASK_SUCCESS
        );
        return flux.transform(
            responses -> CtrlResponseUtils.combineResponses(
                errorReporter,
                responses,
                LinstorParsingUtils.asRscName(rscName),
                "Abort backups of {1} on {0} started"
            )
        ).concatWith(Flux.just(success));
    }

    public Flux<BackupInfoPojo> backupInfo(
        String srcRscName,
        String srcSnapName,
        String backupId,
        Map<String, String> storPoolMapRef,
        @Nullable String nodeName,
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

    /**
     * Find out how if a backup-restore is possible and how much space it would need
     */
    private Flux<BackupInfoPojo> backupInfoInTransaction(
        String srcRscName,
        String srcSnapName,
        String backupId,
        Map<String, String> renameMap,
        @Nullable String nodeName,
        String remoteName
    ) throws AccessDeniedException, InvalidNameException
    {
        S3Remote remote = backupHelper.getS3Remote(remoteName);
        S3MetafileNameInfo metaFile = null;
        byte[] masterKey = backupHelper.getLocalMasterKey();

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
                // do not use backupHelper.getAllS3Keys here to avoid two listObjects calls since objects is needed
                // later
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
            // do not use backupHelper.getAllS3Keys here to avoid two listObjects calls since objects is needed later
            s3keys = objects.stream().map(S3ObjectSummary::getKey).collect(Collectors.toCollection(TreeSet::new));
            metaFile = backupHelper.getLatestBackup(s3keys, srcSnapName);
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
            PairNonNull<Long, Long> totalSizes = new PairNonNull<>(0L, 0L); // dlSize, allocSize
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

    /**
     * Calculate how much free space would be left over after a restore
     */
    Map<String, Long> getRemainingSize(
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

    /**
     * Collect all the info needed for backupInfo
     */
    void fillBackupInfo(
        boolean first,
        Map<StorPoolApi, List<BackupInfoVlmPojo>> storPoolMap,
        List<S3ObjectSummary> objects,
        BackupMetaDataPojo meta,
        RscLayerDataApi layerData,
        PairNonNull<Long, Long> totalSizes
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
