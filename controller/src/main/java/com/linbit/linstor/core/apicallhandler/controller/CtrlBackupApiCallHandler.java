package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.BackupToS3;
import com.linbit.linstor.api.pojo.backups.BackupInfoPojo;
import com.linbit.linstor.api.pojo.backups.BackupListPojo;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apis.BackupListApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlBackupApiCallHandler
{
    private static final DateFormat SDF = new SimpleDateFormat("yyyyMMdd_HHmmss");
    private static final String SNAP_PREFIX = "back_";
    private static final Pattern META_FILE_PATTERN = Pattern
        .compile("^([a-zA-Z0-9_-]{2,48})_(back_[0-9]{8}_[0-9]{6})\\.meta$");
    // TODO: fix pattern for incremetal backups
    private static final Pattern BACKUP_KEY_PATTERN = Pattern
        .compile("^([a-zA-Z0-9_-]{2,48})(\\..+)?_([0-9]{5})_(back_[0-9]{8}_[0-9]{6})$");
    private Provider<AccessContext> peerAccCtx;
    private CtrlApiDataLoader ctrlApiDataLoader;
    private CtrlSnapshotCrtHelper snapshotCrtHelper;
    private CtrlSnapshotCrtApiCallHandler snapshotCrtHandler;
    private ScopeRunner scopeRunner;
    private LockGuardFactory lockGuardFactory;
    private CtrlTransactionHelper ctrlTransactionHelper;
    private ErrorReporter errorReporter;
    private CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private BackupToS3 backupHandler;
    private CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteApiCallHandler;

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
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteApiCallHandlerRef
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
    }

    public Flux<ApiCallRc> createFullBackup(String rscNameRef) throws AccessDeniedException
    {
        Date now = new Date();

        Flux<ApiCallRc> response = scopeRunner.fluxInTransactionalScope(
            "Backup snapshot",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> backupSnapshot(rscNameRef, SNAP_PREFIX + SDF.format(now))
        );

        return response;
    }

    private Flux<ApiCallRc> backupSnapshot(String rscNameRef, String snapName)
        throws AccessDeniedException
    {
        try
        {
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
            Collection<SnapshotDefinition> snapDfns = rscDfn.getSnapshotDfns(peerAccCtx.get());
            for (SnapshotDefinition snapDfn : snapDfns)
            {
                if (
                    snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING) &&
                        snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.BACKUP)
                )
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_EXISTS_SNAPSHOT_SHIPPING,
                            "Backup shipping of resource '" + rscNameRef + "' already in progress"
                        )
                    );
                }
            }
            Iterator<Resource> rscIt = rscDfn.iterateResource(peerAccCtx.get());
            List<String> nodes = new ArrayList<>();
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                if (
                    !rsc.getStateFlags()
                        .isSomeSet(peerAccCtx.get(), Resource.Flags.DRBD_DISKLESS, Resource.Flags.NVME_INITIATOR) &&
                        backupShippingSupported(rsc).isEmpty()
                )
                {
                    nodes.add(rsc.getNode().getName().displayValue);
                }
            }
            if (nodes.size() == 0)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_ENOUGH_NODES,
                        "Backup shipping of resource '" + rscDfn.getName().displayValue +
                            "' cannot be started since there is no node available that supports backup shipping."
                    )
                );
            }
            NodeName chosenNode = new NodeName(nodes.get(0));
            ApiCallRcImpl responses = new ApiCallRcImpl();
            SnapshotDefinition snapDfn = snapshotCrtHelper
                .createSnapshots(nodes, rscDfn.getName().displayValue, snapName, responses);
            snapDfn.getFlags()
                .enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING, SnapshotDefinition.Flags.BACKUP);
            snapDfn.getProps(peerAccCtx.get())
                .setProp(InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP, snapName, ApiConsts.NAMESPC_BACKUP_SHIPPING);

            snapDfn.getSnapshot(peerAccCtx.get(), chosenNode).getFlags()
                .enableFlags(peerAccCtx.get(), Snapshot.Flags.BACKUP_SOURCE);
            ctrlTransactionHelper.commit();
            return snapshotCrtHandler.postCreateSnapshot(snapDfn);
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

    public Flux<ApiCallRc> deleteBackup(
        String rscName,
        String snapName,
        String timestamp,
        List<String> s3keys,
        boolean external
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Backup snapshot",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> deleteBackupInTransaction(rscName, snapName, timestamp, s3keys, external)
        );
    }

    private Flux<ApiCallRc> deleteBackupInTransaction(
        String rscName,
        String snapName,
        String timestamp,
        List<String> s3keys,
        boolean external
    )
    {
        ToDeleteCollections toDelete = new ToDeleteCollections();
        if (rscName.length() != 0)
        {
            if (snapName.length() != 0)
            {
                toDelete.s3keys = getKeysFromMeta(rscName + "_" + snapName + ".meta");
                toDelete.snapKeys = Collections.singleton(
                    toSnapDfnKey(rscName, snapName)
                );
            }
            else
            {
                toDelete = getDeleteList(rscName, timestamp);
            }
        }
        else if (s3keys.size() != 0)
        {
            if (external)
            {
                toDelete.s3keys = new TreeSet<>(s3keys);
            }
            else
            {
                toDelete = getDeleteList(s3keys);
            }
        }
        else
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REQUEST | ApiConsts.MASK_BACKUP,
                    "Missing parameters to complete deletion. Either specify a resource name or an s3key."
                )
            );
        }
        Flux<ApiCallRc> deleteSnapFlux = Flux.empty();
        for (SnapshotDefinition.Key snapKey : toDelete.snapKeys)
        {
            deleteSnapFlux = deleteSnapFlux.concatWith(
                ctrlSnapDeleteApiCallHandler
                    .deleteSnapshot(snapKey.getResourceName().displayValue, snapKey.getSnapshotName().displayValue)
            );
        }
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            if (!toDelete.s3keys.isEmpty())
            {
                backupHandler.deleteObjects(toDelete.s3keys);
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
                "Could not delete " + toDelete.s3keys.toString(), ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP
            );
            toDelete.s3keys = deletedKeys;
        }
        apiCallRc.addEntry(
            "Successfully deleted " + toDelete.s3keys.toString(), ApiConsts.MASK_SUCCESS | ApiConsts.MASK_BACKUP
        );
        apiCallRc.addEntries(toDelete.apiCallRcs);
        return Flux.<ApiCallRc> just(apiCallRc).concatWith(deleteSnapFlux);
    }

    private ToDeleteCollections getDeleteList(List<String> s3keysRef)
    {
        ToDeleteCollections ret = new ToDeleteCollections();
        for (String s3keyRef : s3keysRef)
        {
            Matcher mBack = BACKUP_KEY_PATTERN.matcher(s3keyRef);
            Matcher mMeta = META_FILE_PATTERN.matcher(s3keyRef);
            if (mBack.matches())
            {
                ret.s3keys.addAll(getKeysFromMeta(mBack.group(1) + "_" + mBack.group(4) + ".meta"));
                ret.snapKeys.add(toSnapDfnKey(mBack.group(1), mBack.group(4)));
            }
            else if (mMeta.matches())
            {
                ret.s3keys.addAll(getKeysFromMeta(s3keyRef));
                ret.snapKeys.add(toSnapDfnKey(mMeta.group(1), mMeta.group(2)));
            }
            else
            {
                ret.apiCallRcs.addEntry(
                    "The key " + s3keyRef +
                        " is not a valid backup name. Please check your input or consider using the external-option.",
                    ApiConsts.FAIL_INVLD_REQUEST | ApiConsts.MASK_BACKUP
                );
            }
        }
        return ret;
    }

    private ToDeleteCollections getDeleteList(String rscName, String timestamp)
    {
        Set<String> metaKeys = new HashSet<>();
        Set<String> s3keys = backupHandler.listObjects(rscName, "").stream().map(S3ObjectSummary::getKey)
            .collect(Collectors.toCollection(TreeSet::new));
        ToDeleteCollections ret = new ToDeleteCollections();
        for (String s3key : s3keys)
        {
            Matcher m = META_FILE_PATTERN.matcher(s3key);
            if (m.matches())
            {
                // get rid of the "back_" prefix
                String ts = m.group(2).substring(5);
                try
                {
                    if (timestamp.isEmpty() || SDF.parse(timestamp).after(SDF.parse(ts)))
                    {
                        metaKeys.add(s3key);
                        ret.snapKeys.add(toSnapDfnKey(m.group(1), m.group(2)));
                    }
                }
                catch (ParseException exc)
                {
                    errorReporter.reportError(exc, peerAccCtx.get(), null, "used s3 key: " + s3key);
                }
            }
        }
        for (String metaName : metaKeys)
        {
            ret.s3keys.addAll(getKeysFromMeta(metaName));
        }
        return ret;
    }

    private Set<String> getKeysFromMeta(String metaName)
    {
        Set<String> keys = new TreeSet<>();
        try
        {
            BackupMetaDataPojo metadata = backupHandler.getMetaFile(metaName, "");
            List<List<BackupInfoPojo>> backInfoLists = metadata.getBackups();
            for (List<BackupInfoPojo> backInfoList : backInfoLists)
            {
                for (BackupInfoPojo backInfo : backInfoList)
                {
                    keys.add(backInfo.getName());
                }
            }
            keys.add(metaName);
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc, peerAccCtx.get(), null, "used s3 key: " + metaName);
        }
        return keys;
    }

    private SnapshotDefinition.Key toSnapDfnKey(String rscName, String snapName)
    {
        return new SnapshotDefinition.Key(
            LinstorParsingUtils.asRscName(rscName),
            LinstorParsingUtils.asSnapshotName(snapName)
        );
    }

    public Pair<Collection<BackupListApi>, Set<String>> listBackups(String rscNameRef, String bucketNameRef)
    {
        List<S3ObjectSummary> objects = backupHandler.listObjects(rscNameRef, bucketNameRef);
        Set<String> s3keys = objects.stream().map(S3ObjectSummary::getKey)
            .collect(Collectors.toCollection(TreeSet::new));
        Map<String, BackupListApi> backups = new TreeMap<>();
        Set<String> usedKeys = new TreeSet<>();

        // add all backups to the list that have useable metadata-files
        for (String s3key : s3keys)
        {
            Matcher m = META_FILE_PATTERN.matcher(s3key);
            if (m.matches())
            {
                try
                {
                    BackupMetaDataPojo metadata = backupHandler.getMetaFile(s3key, bucketNameRef);
                    List<List<BackupInfoPojo>> backInfoLists = metadata.getBackups();
                    BackupInfoPojo firstBackInfo = backInfoLists.get(0).get(0);
                    Map<String, String> vlms = new TreeMap<>();
                    boolean restoreable = true;

                    for (List<BackupInfoPojo> backInfoList : backInfoLists)
                    {
                        // TODO: iterate over lists to get incs
                        BackupInfoPojo backInfo = backInfoList.get(0);
                        Matcher mKey = BACKUP_KEY_PATTERN.matcher(backInfo.getName());
                        if (mKey.matches() && s3keys.contains(backInfo.getName()))
                        {
                            vlms.put("" + Integer.parseInt(mKey.group(3)), backInfo.getName());
                            usedKeys.add(backInfo.getName());
                        }
                        else
                        {
                            // meta-file corrupt
                            restoreable = false;
                        }
                    }
                    // TODO: add & fill inc-list
                    BackupListApi back = new BackupListPojo(
                        m.group(2),
                        s3key,
                        firstBackInfo.getFinishedTime(),
                        SDF.parse(firstBackInfo.getFinishedTime()).getTime(),
                        firstBackInfo.getNode(),
                        false,
                        true,
                        vlms,
                        null
                    );
                    // get rid of ".meta"
                    backups.put(s3key.substring(0, s3key.length() - 5), back);
                    usedKeys.add(s3key);
                }
                catch (IOException | ParseException exc)
                {
                    errorReporter.reportError(exc, peerAccCtx.get(), null, "used s3 key: " + s3key);
                }
            }
        }
        s3keys.removeAll(usedKeys);
        usedKeys.clear();

        // add all backups to the list that look like backups, and maybe even have a rscDfn/snapDfn, but are not in a
        // metadata-file
        for (String s3key : s3keys)
        {
            if (!usedKeys.contains(s3key))
            {
                Matcher m = BACKUP_KEY_PATTERN.matcher(s3key);
                // TODO: match for inc, if inc found:
                // see if rsc already has an entry
                // if so, get inc-list from that and add a new entry
                if (m.matches())
                {
                    String rscName = m.group(1);
                    String snapName = m.group(4);
                    ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, false);
                    if (rscDfn != null)
                    {
                        try
                        {
                            SnapshotDefinition snapDfn = rscDfn
                                .getSnapshotDfn(peerAccCtx.get(), new SnapshotName(snapName));
                            if (snapDfn != null)
                            {
                                BackupListApi back = fillBackupListPojo(
                                    s3key, rscName, snapName, m, BACKUP_KEY_PATTERN, s3keys, usedKeys, snapDfn
                                );
                                backups.put(s3key.substring(0, s3key.length() - 5), back);
                            }
                            else
                            {
                                BackupListApi back = fillBackupListPojo(
                                    s3key, rscName, snapName, m, BACKUP_KEY_PATTERN, s3keys, usedKeys, null
                                );
                                backups.put(s3key.substring(0, s3key.length() - 5), back);
                            }
                        }
                        catch (InvalidNameException exc)
                        {
                            throw new ImplementationError(exc);
                        }
                        catch (AccessDeniedException exc)
                        {
                            // no access to snapDfn
                        }
                    }
                    else
                    {
                        BackupListApi back = fillBackupListPojo(
                            s3key, rscName, snapName, m, BACKUP_KEY_PATTERN, s3keys, usedKeys, null
                        );
                        backups.put(s3key.substring(0, s3key.length() - 5), back);
                    }

                    usedKeys.add(s3key);
                }
            }
        }
        s3keys.removeAll(usedKeys);
        return new Pair<>(backups.values(), s3keys);
    }

    private BackupListApi fillBackupListPojo(
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
        Map<String, String> vlms = new TreeMap<>();
        vlms.put("" + Integer.parseInt(m.group(3)), s3key);
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
                    vlms.put("" + Integer.parseInt(matcher.group(3)), otherKey);
                    usedKeys.add(otherKey);
                }
            }
        }
        // TODO: for inc of this full-backup:
        // get all other keys that start with rscName, don't contain snapName and are
        // incremental
        Boolean shipping;
        Boolean success;
        try
        {
            if (snapDfn != null && snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.BACKUP))
            {
                if (snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING))
                {
                    shipping = true;
                    success = null;
                }
                else if (
                    snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED)
                )
                {
                    shipping = false;
                    success = true;
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

        BackupListApi back = new BackupListPojo(
            snapName,
            rscName + "_" + snapName + ".meta",
            null,
            null,
            null,
            shipping,
            success,
            vlms,
            null
        );
        // get rid of ".meta"
        return back;
    }

    public Flux<ApiCallRc> shippingReceived(String rscNameRef, String snapNameRef, boolean successRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Finish received snapshot-shipping",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> shippingReceivedInTransaction(rscNameRef, snapNameRef, successRef)
            );
    }

    private Flux<ApiCallRc> shippingReceivedInTransaction(String rscNameRef, String snapNameRef, boolean successRef)
    {
        errorReporter.logInfo(
            "Backup shipping for snapshot %s of resource %s %s", snapNameRef, rscNameRef,
            successRef ? "finished successfully" : "failed"
        );
        SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameRef, snapNameRef, true);
        try
        {
            snapDfn.getFlags().disableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING);
            if (successRef)
            {
                snapDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPED);
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
        catch (AccessDeniedException exc)
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

    private static class ToDeleteCollections
    {
        Set<String> s3keys;
        Set<SnapshotDefinition.Key> snapKeys;
        ApiCallRcImpl apiCallRcs;

        ToDeleteCollections()
        {
            s3keys = new TreeSet<>();
            snapKeys = new TreeSet<>();
            apiCallRcs = new ApiCallRcImpl();
        }
    }
}
