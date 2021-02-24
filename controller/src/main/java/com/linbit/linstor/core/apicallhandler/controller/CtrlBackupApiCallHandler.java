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
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
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
import com.linbit.linstor.core.repository.NodeRepository;
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
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlBackupApiCallHandler
{
    private static final DateFormat SDF = new SimpleDateFormat("yyyyMMdd_HHmmss");
    private static final String SNAP_PREFIX = "back_";
    private Provider<AccessContext> peerAccCtx;
    private CtrlApiDataLoader ctrlApiDataLoader;
    private CtrlSnapshotCrtHelper snapshotCrtHelper;
    private CtrlSnapshotCrtApiCallHandler snapshotCrtHandler;
    private ScopeRunner scopeRunner;
    private LockGuardFactory lockGuardFactory;
    private CtrlTransactionHelper ctrlTransactionHelper;
    private NodeRepository nodeRepository;
    private CtrlStltSerializer stltComSerializer;
    private ErrorReporter errorReporter;
    private CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private BackupToS3 backupHandler;

    @Inject
    public CtrlBackupApiCallHandler(
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSnapshotCrtHelper snapCrtHelperRef,
        CtrlSnapshotCrtApiCallHandler snapshotCrtHandlerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        NodeRepository nodeRepositoryRef,
        CtrlStltSerializer stltComSerializerRef,
        ErrorReporter errorReporterRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        BackupToS3 backupHandlerRef
    )
    {
        peerAccCtx = peerAccCtxRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        snapshotCrtHelper = snapCrtHelperRef;
        snapshotCrtHandler = snapshotCrtHandlerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        nodeRepository = nodeRepositoryRef;
        stltComSerializer = stltComSerializerRef;
        errorReporter = errorReporterRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        backupHandler = backupHandlerRef;

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

    public Pair<Collection<BackupListApi>, Set<String>> listBackups(String rscNameRef, String bucketNameRef)
    {
        List<S3ObjectSummary> objects = backupHandler.listObjects(rscNameRef, bucketNameRef);
        Set<String> s3keys = objects.stream().map(S3ObjectSummary::getKey)
            .collect(Collectors.toCollection(TreeSet::new));
        Pattern metaFilePattern = Pattern.compile("^([a-zA-Z0-9_-]{2,48})_(back_[0-9]{8}_[0-9]{6})\\.meta$");
        // TODO: fix pattern for incremetal backups
        Pattern backupKeyPattern = Pattern
            .compile("^([a-zA-Z0-9_-]{2,48})(\\..+)?_([0-9]{5})_(back_[0-9]{8}_[0-9]{6})$");
        Map<String, BackupListApi> backups = new TreeMap<>();
        Set<String> usedKeys = new TreeSet<>();

        // add all backups to the list that have useable metadata-files
        for (String s3key : s3keys)
        {
            Matcher m = metaFilePattern.matcher(s3key);
            if (m.matches())
            {
                try
                {
                    BackupMetaDataPojo metadata = backupHandler.getMetaFile(s3key, bucketNameRef);
                    List<List<BackupInfoPojo>> backInfoLists = metadata.getBackups();
                    BackupInfoPojo firstBackInfo = backInfoLists.get(0).get(0);
                    Map<String, String> vlms = new TreeMap<>();

                    for (List<BackupInfoPojo> backInfoList : backInfoLists)
                    {
                        // TODO: iterate over lists to get incs
                        BackupInfoPojo backInfo = backInfoList.get(0);
                        Matcher mKey = backupKeyPattern.matcher(backInfo.getName());
                        if (mKey.matches() && s3keys.contains(backInfo.getName()))
                        {
                            vlms.put("" + Integer.parseInt(mKey.group(3)), backInfo.getName());
                            usedKeys.add(backInfo.getName());
                        }
                        else
                        {
                            // meta-file corrupt
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
                Matcher m = backupKeyPattern.matcher(s3key);
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
                                    s3key, rscName, snapName, m, backupKeyPattern, s3keys, usedKeys, snapDfn
                                );
                                backups.put(s3key.substring(0, s3key.length() - 5), back);
                            }
                            else
                            {
                                BackupListApi back = fillBackupListPojo(
                                    s3key, rscName, snapName, m, backupKeyPattern, s3keys, usedKeys, null
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
                            s3key, rscName, snapName, m, backupKeyPattern, s3keys, usedKeys, null
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
                    "Finishing shpipping of backup ''" + snapNameRef + "'' of {1} on {0}"
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
}
