package com.linbit.linstor.core.apicallhandler.controller.backup;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.BackupToS3;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.backupshipping.S3MetafileNameInfo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Schedule;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsLayerHelperUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.tasks.ScheduleBackupService;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlScheduledBackupsApiCallHandler
{
    private static final String SCHEDULE_KEY = InternalApiConsts.NAMESPC_SCHEDULE + ReadOnlyProps.PATH_SEPARATOR
        + InternalApiConsts.KEY_BACKUP_SHIPPED_BY_SCHEDULE;

    private final Provider<ScheduleBackupService> scheduleService;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteApiCallHandler;
    private final ErrorReporter errorReporter;
    private final CtrlBackupApiHelper backupHelper;
    private final BackupToS3 backupToS3Handler;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final SystemConfRepository systemConfRepository;
    private final ResourceDefinitionRepository rscDfnRepo;
    private final CtrlBackupApiCallHandler backupApiCallHandler;

    @Inject
    public CtrlScheduledBackupsApiCallHandler(
        Provider<ScheduleBackupService> scheduleServiceRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteApiCallHandlerRef,
        ErrorReporter errorReporterRef,
        CtrlBackupApiHelper backupHelperRef,
        BackupToS3 backupToS3HandlerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        SystemConfRepository systemConfRepositoryRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        CtrlBackupApiCallHandler backupApiCallHandlerRef
    )
    {
        scheduleService = scheduleServiceRef;
        ctrlSnapDeleteApiCallHandler = ctrlSnapDeleteApiCallHandlerRef;
        errorReporter = errorReporterRef;
        backupHelper = backupHelperRef;
        backupToS3Handler = backupToS3HandlerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        systemConfRepository = systemConfRepositoryRef;
        rscDfnRepo = rscDfnRepoRef;
        backupApiCallHandler = backupApiCallHandlerRef;

    }

    /**
     * Makes sure the scheduled-shipping-task gets re-added correctly
     */
    public boolean rescheduleShipping(
        SnapshotDefinition snapDfn,
        NodeName nodeName,
        ResourceDefinition rscDfn,
        Schedule schedule,
        AbsRemote remote,
        boolean success,
        boolean forceSkip
    ) throws InvalidKeyException
    {
        String namespace = BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + remote.getName().displayValue;
        boolean lastBackupIncremental = snapDfn.getSnapDfnProps()
            .getProp(InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT, namespace) != null;
        String backupTimeRaw = snapDfn.getSnapDfnProps()
            .getProp(InternalApiConsts.KEY_BACKUP_START_TIMESTAMP, namespace);
        scheduleService.get().addTaskAgain(
            rscDfn,
            schedule,
            remote,
            Long.parseLong(backupTimeRaw),
            success,
            forceSkip,
            lastBackupIncremental
        );
        return lastBackupIncremental;
    }

    /**
     * Checks and if needed deletes snaps and backups as specified in the schedule
     */
    public Flux<ApiCallRc> checkScheduleKeep(ResourceDefinition rscDfn, Schedule schedule, AbsRemote s3orLinRemote)
        throws IOException
    {
        Flux<ApiCallRc> deleteFlux = Flux.empty();
        deleteFlux = deleteFlux.concatWith(checkKeepLocal(rscDfn, schedule, s3orLinRemote));
        if (s3orLinRemote instanceof S3Remote s3Remote)
        {
            deleteFlux = deleteFlux
                .concatWith(checkKeepRemote(rscDfn.getName().displayValue, s3Remote, schedule));
        }

        return deleteFlux;
    }

    /**
     * Finds out how many full-backup-base-snaps exist locally, and if that number is too big.
     * If it is, deletes the failed ones first, and if there are no failed snaps, the oldest.
     */
    private Flux<ApiCallRc> checkKeepLocal(ResourceDefinition rscDfn, Schedule schedule, AbsRemote s3orLinRemote)
    {
        String s3orLinRemoteName = s3orLinRemote.getName().displayValue;
        Flux<ApiCallRc> deleteFlux = Flux.empty();
        List<SnapshotDefinition> snapDfnsToCheck = getFullBackupBaseSnaps(
            rscDfn,
            schedule.getName().displayValue,
            s3orLinRemoteName
        );
        Integer keepLocal = schedule.getKeepLocal();
        if (keepLocal != null && keepLocal < snapDfnsToCheck.size())
        {
            int numToDelete = snapDfnsToCheck.size() - keepLocal;
            snapDfnsToCheck.sort((a, b) ->
            {
                String ts2;
                boolean success2;
                String ts1 = a.getSnapDfnProps()
                    .getProp(
                        InternalApiConsts.KEY_BACKUP_START_TIMESTAMP,
                        BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + s3orLinRemoteName
                    );
                boolean success1 = BackupShippingUtils.hasShippingStatus(
                    a,
                    s3orLinRemoteName,
                    InternalApiConsts.VALUE_SUCCESS
                );
                ts2 = b.getSnapDfnProps()
                    .getProp(
                        InternalApiConsts.KEY_BACKUP_START_TIMESTAMP,
                        BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + s3orLinRemoteName
                    );
                success2 = BackupShippingUtils.hasShippingStatus(
                    b,
                    s3orLinRemoteName,
                    InternalApiConsts.VALUE_SUCCESS
                );
                int successComp = Boolean.compare(success1, success2);
                return successComp == 0 ? Long.compare(Long.parseLong(ts1), Long.parseLong(ts2)) : successComp;
            });

            List<SnapshotDefinition> fullSnapsToDel = new ArrayList<>();
            // sorted: failed from oldest to newest, then successful from oldest to newest
            for (SnapshotDefinition snapDfnToCheck : snapDfnsToCheck)
            {
                if (numToDelete > 0)
                {
                    numToDelete--;
                    errorReporter.logTrace(
                        "SnapDfn %s and all snapDfns dependent on it will be deleted due to schedule %s",
                        snapDfnToCheck.getName(),
                        schedule.getName()
                    );
                    fullSnapsToDel.add(snapDfnToCheck);
                }
            }
            Map<SnapshotDefinition, List<SnapshotDefinition>> chains = getAllSnapshotChains(
                rscDfn,
                schedule.getName().displayValue,
                s3orLinRemote.getName().displayValue,
                s3orLinRemoteName
            );
            for (SnapshotDefinition fullSnapToDel : fullSnapsToDel)
            {
                List<SnapshotDefinition> chain = chains.get(fullSnapToDel);
                if (chain != null)
                {
                    for (SnapshotDefinition incSnapToDel : chain)
                    {
                        deleteFlux = deleteFlux.concatWith(
                            ctrlSnapDeleteApiCallHandler.deleteSnapshot(
                                incSnapToDel.getResourceName(),
                                incSnapToDel.getName(),
                                null
                            )
                        );
                    }
                }
                deleteFlux = deleteFlux.concatWith(
                    ctrlSnapDeleteApiCallHandler.deleteSnapshot(
                        fullSnapToDel.getResourceName(),
                        fullSnapToDel.getName(),
                        null
                    )
                );
            }
        }
        return deleteFlux;
    }

    /**
     * Finds out how many full backups there are on the given remote and if that number is too big.
     * If it is, deletes the oldest ones first.
     */
    private Flux<ApiCallRc> checkKeepRemote(String rscName, S3Remote remote, Schedule schedule)
        throws IOException
    {
        Flux<ApiCallRc> deleteFlux = Flux.empty();
        Map<LocalDateTime, String> backupsToCheck = getFullBackupS3Keys(
            rscName, remote, schedule.getName().displayValue
        );
        Integer keepRemote = schedule.getKeepRemote();

        if (keepRemote != null && keepRemote < backupsToCheck.size())
        {
            int numToDelete = backupsToCheck.size() - keepRemote;
            // backupsToCheck: Date sorts from oldest to newest
            for (String s3key : backupsToCheck.values())
            {
                if (numToDelete > 0)
                {
                    numToDelete--;
                    errorReporter.logTrace(
                        "Backup %s and all backups dependent on it will be deleted on remote %s due to schedule %s",
                        s3key,
                        remote.getName(),
                        schedule.getName()
                    );
                    deleteFlux = backupApiCallHandler.deleteBackup(
                        rscName,
                        s3key,
                        null,
                        null,
                        null,
                        true,
                        false,
                        false,
                        null,
                        remote.getName().displayValue,
                        false,
                        true
                    );
                }
                else
                {
                    break;
                }
            }
        }
        return deleteFlux;
    }

    /**
     * Returns a map of full-backup-base-snaps as keys to a list of inc-backup-base-snaps belonging to that
     * full-backup-base-snap as values.
     */
    private Map<SnapshotDefinition, List<SnapshotDefinition>> getAllSnapshotChains(
        ResourceDefinition rscDfn,
        String scheduleName,
        String s3orStltRemoteName,
        String s3orLinRemoteName
    )
    {
        TreeMap<String, SnapshotDefinition> sourceSnaps = new TreeMap<>();
        for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns())
        {
            ReadOnlyProps props = snapDfn.getSnapDfnProps();
            String schedule = props.getProp(SCHEDULE_KEY);
            String s3orStltRemote = props.getProp(
                BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + s3orLinRemoteName + "/" +
                    InternalApiConsts.KEY_BACKUP_TARGET_REMOTE
            );
            if (
                schedule != null && schedule.equals(scheduleName) && s3orStltRemote != null && s3orStltRemote.equals(
                    s3orStltRemoteName
                )
            )
            {
                sourceSnaps.put(snapDfn.getName().displayValue, snapDfn);
            }
        }
        Map<SnapshotDefinition, List<SnapshotDefinition>> chains = new TreeMap<>();
        final String PREV_BACKUP_KEY = BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC +
            ReadOnlyProps.PATH_SEPARATOR + s3orLinRemoteName + ReadOnlyProps.PATH_SEPARATOR +
            InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT;
        while (!sourceSnaps.isEmpty())
        {
            List<SnapshotDefinition> chain = new ArrayList<>();
            SnapshotDefinition startSnap = sourceSnaps.pollFirstEntry().getValue();
            Props startSnapDfnProps = startSnap.getSnapDfnProps();
            String prevFullSnap = startSnapDfnProps.getProp(
                BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + s3orLinRemoteName + "/" +
                    InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP
            );
            String prevSnap = startSnapDfnProps.getProp(PREV_BACKUP_KEY);
            if (!prevFullSnap.equalsIgnoreCase(startSnap.getName().value))
            {
                while (prevSnap != null)
                {
                    chain.add(startSnap);
                    startSnap = sourceSnaps.remove(prevSnap);
                    if (startSnap == null)
                    {
                        for (Entry<SnapshotDefinition, List<SnapshotDefinition>> chainEntry : chains.entrySet())
                        {
                            if (
                                !chainEntry.getValue().isEmpty() &&
                                    chainEntry.getValue().get(0).getName().value.equalsIgnoreCase(prevSnap)
                            )
                            {
                                // we previously started in the middle of the chain and need to add both chains together
                                prevSnap = null;
                                startSnap = chainEntry.getKey();
                                chain.addAll(chainEntry.getValue());
                                break;
                            }
                            else if (
                                chainEntry.getValue().isEmpty() &&
                                    chainEntry.getKey().getName().value.equalsIgnoreCase(prevSnap)
                            )
                            {
                                // we previously found only the full-base, and need to overwrite the empty list we added
                                // to it
                                prevSnap = null;
                                startSnap = chainEntry.getKey();
                                break;
                            }
                        }
                        if (startSnap == null)
                        {
                            // we have a part-chain that has a deleted snap as next element, ignore this chain and start
                            // with a new one
                            prevSnap = null;
                        }
                    }
                    else
                    {
                        if (prevFullSnap.equalsIgnoreCase(startSnap.getName().value))
                        {
                            // we reached a full-base, stop the loop
                            prevSnap = null;
                        }
                        else
                        {
                            prevSnap = startSnapDfnProps.getProp(PREV_BACKUP_KEY);
                            prevFullSnap = startSnapDfnProps.getProp(
                                BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + s3orLinRemoteName + "/" +
                                    InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP
                            );
                        }
                    }
                }
            }
            if (startSnap != null)
            {
                chains.put(startSnap, chain);
            }
        }
        return chains;
    }

    /**
     * Find all snapDfns of the given triple that were the basis for a full backup
     */
    private List<SnapshotDefinition> getFullBackupBaseSnaps(
        ResourceDefinition rscDfn,
        String scheduleName,
        String s3orLinRemoteName
    )
    {
        List<SnapshotDefinition> snapDfns = new ArrayList<>();
        for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns())
        {
            ReadOnlyProps props = snapDfn.getSnapDfnProps();
            if (
                isFullBackupOfSchedule(
                    props.map(),
                    scheduleName,
                    s3orLinRemoteName,
                    snapDfn.getName().displayValue
                ) && props.getProp(
                    InternalApiConsts.KEY_BACKUP_START_TIMESTAMP,
                    BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + s3orLinRemoteName
                ) != null
            )
            {
                snapDfns.add(snapDfn);
            }
        }
        return snapDfns;
    }

    /**
     * Find all backups of the given triple that are full backups
     */
    private Map<LocalDateTime, String> getFullBackupS3Keys(String rscNameRef, S3Remote remote, String scheduleName)
        throws IOException
    {
        Map<LocalDateTime, String> s3keysRet = new TreeMap<>();
        Set<String> s3keys = backupHelper.getAllS3Keys(remote, rscNameRef);
        for (String s3key : s3keys)
        {
            try
            {
                S3MetafileNameInfo info = new S3MetafileNameInfo(s3key);
                BackupMetaDataPojo s3MetaFile = backupToS3Handler
                    .getMetaFile(s3key, remote, backupHelper.getLocalMasterKey());
                if (
                    isFullBackupOfSchedule(
                        s3MetaFile.getRscDfn().getSnapDfnProps(),
                        scheduleName,
                        remote.getName().displayValue,
                        info.snapName
                    )
                )
                {
                    s3keysRet.put(info.backupTime, s3key);
                }
            }
            catch (ParseException ignored)
            {
                // not a metafile
            }
        }
        return s3keysRet;
    }

    /**
     * Check the correct flags to find out if the snapshot is a full-backup-basis
     */
    private boolean isFullBackupOfSchedule(
        Map<String, String> props,
        String scheduleName,
        String s3orLinRemoteName,
        String snapName
    )
    {
        String schedule = props.get(SCHEDULE_KEY);
        // if PREV_BACKUP_KEY == snapName it is a full backup
        return schedule != null && schedule.equals(scheduleName) &&
            props.get(
                BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + s3orLinRemoteName + "/" +
                    InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP
            ).equalsIgnoreCase(snapName);
    }

    /**
     * Called by the client to enable a remote-schedule-pair on rscDfn, rscGrp, or ctrl level.
     * Also allows the client to set a prefNode
     */
    public Flux<ApiCallRc> enableSchedule(
        @Nullable String rscNameRef,
        @Nullable String grpNameRef,
        String remoteNameRef,
        String scheduleNameRef,
        @Nullable String nodeNameRef,
        @Nullable String dstStorPool,
        Map<String, String> storpoolRename,
        @Nullable String dstRscGrpRef,
        @Nullable String dstRscNameRef,
        boolean forceRestoreRef,
        boolean forceRscGrpRef
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Enable backup schedule",
            lockGuardFactory.create().write(LockObj.RSC_DFN_MAP, LockObj.RSC_GRP_MAP, LockObj.CTRL_CONFIG)
                .read(LockObj.REMOTE_MAP, LockObj.SCHEDULE_MAP)
                .buildDeferred(),
            () -> setScheduleInTransaction(
                rscNameRef,
                grpNameRef,
                remoteNameRef,
                scheduleNameRef,
                nodeNameRef,
                dstStorPool,
                storpoolRename,
                dstRscGrpRef,
                dstRscNameRef,
                true,
                forceRestoreRef,
                forceRscGrpRef
            )
        );
    }

    /**
     * Called by the client to disable a remote-schedule-pair on rscDfn, rscGrp, or ctrl level.
     */
    public Flux<ApiCallRc> disableSchedule(
        @Nullable String rscNameRef,
        @Nullable String grpNameRef,
        String remoteNameRef,
        String scheduleNameRef,
        @Nullable String nodeNameRef
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Disable backup schedule",
            lockGuardFactory.create().write(LockObj.RSC_DFN_MAP, LockObj.RSC_GRP_MAP, LockObj.CTRL_CONFIG)
                .read(LockObj.REMOTE_MAP, LockObj.SCHEDULE_MAP)
                .buildDeferred(),
            () -> setScheduleInTransaction(
                rscNameRef,
                grpNameRef,
                remoteNameRef,
                scheduleNameRef,
                nodeNameRef,
                null,
                null,
                null,
                null,
                false,
                false,
                false
            )
        );
    }

    /**
     * Sets the correct props to enable or disable the given remote-schedule-pair on rscDfn, rscGrp, or ctrl level
     */
    private Flux<ApiCallRc> setScheduleInTransaction(
        @Nullable String rscNameRef,
        @Nullable String grpNameRef,
        String remoteNameRef,
        String scheduleNameRef,
        @Nullable String nodeNameRef,
        @Nullable String dstStorPool,
        @Nullable Map<String, String> storpoolRename,
        @Nullable String dstRscGrpRef,
        @Nullable String dstRscNameRef,
        boolean add,
        boolean forceRestore,
        boolean forceRscGrp
    ) throws InvalidKeyException, DatabaseException, InvalidValueException
    {
        ApiCallRcImpl response = new ApiCallRcImpl();
        AbsRemote remote = ctrlApiDataLoader.loadRemote(remoteNameRef);
        Schedule schedule = ctrlApiDataLoader.loadSchedule(scheduleNameRef);
        List<ResourceDefinition> rscDfnsToCheck = new ArrayList<>();
        Map<String, String> renameMap = new HashMap<>();
        if (storpoolRename != null)
        {
            renameMap.putAll(storpoolRename);
        }
        if (dstStorPool != null && !dstStorPool.isEmpty())
        {
            renameMap.put(AbsLayerHelperUtils.RENAME_STOR_POOL_DFLT_KEY, dstStorPool);
        }
        final String namespace = InternalApiConsts.NAMESPC_SCHEDULE + ReadOnlyProps.PATH_SEPARATOR +
            remote.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR + schedule.getName().displayValue +
            ReadOnlyProps.PATH_SEPARATOR;
        if (rscNameRef != null && !rscNameRef.isEmpty())
        {
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef);
            Props propsRef = rscDfn.getProps();
            propsRef.setProp(
                InternalApiConsts.KEY_TRIPLE_ENABLED,
                add ? ApiConsts.VAL_TRUE : ApiConsts.VAL_FALSE,
                namespace
            );
            if (nodeNameRef != null && !nodeNameRef.isEmpty())
            {
                propsRef.setProp(
                    InternalApiConsts.KEY_SCHEDULE_PREF_NODE,
                    nodeNameRef,
                    namespace
                );
            }
            if (dstRscGrpRef != null && !dstRscGrpRef.isEmpty())
            {
                propsRef.setProp(
                    InternalApiConsts.KEY_SCHEDULE_DST_RSC_GRP,
                    dstRscGrpRef,
                    namespace
                );
                if (forceRscGrp)
                {
                    propsRef.setProp(
                        InternalApiConsts.KEY_SCHEDULE_DST_RSC_GRP_FORCE,
                        ApiConsts.VAL_TRUE,
                        namespace
                    );
                }
            }
            if (dstRscNameRef != null && !dstRscNameRef.isEmpty())
            {
                propsRef.setProp(
                    InternalApiConsts.KEY_SCHEDULE_DST_RSC_NAME,
                    dstRscNameRef,
                    namespace
                );
            }
            for (Entry<String, String> renameEntry : renameMap.entrySet())
            {
                propsRef.setProp(
                    InternalApiConsts.KEY_RENAME_STORPOOL_MAP + ReadOnlyProps.PATH_SEPARATOR + renameEntry.getKey(),
                    renameEntry.getValue(),
                    namespace
                );
            }
            if (add && forceRestore)
            {
                propsRef.setProp(
                    InternalApiConsts.KEY_FORCE_RESTORE,
                    ApiConsts.VAL_TRUE,
                    namespace
                );
            }
            rscDfnsToCheck.add(rscDfn);
        }
        else if (grpNameRef != null && !grpNameRef.isEmpty())
        {
            ResourceGroup rscGrp = ctrlApiDataLoader.loadResourceGroup(grpNameRef);
            Props propsRef = rscGrp.getProps();
            propsRef.setProp(
                InternalApiConsts.KEY_TRIPLE_ENABLED,
                add ? ApiConsts.VAL_TRUE : ApiConsts.VAL_FALSE,
                namespace
            );
            if (nodeNameRef != null && !nodeNameRef.isEmpty())
            {
                propsRef.setProp(
                    InternalApiConsts.KEY_SCHEDULE_PREF_NODE,
                    nodeNameRef,
                    namespace
                );
            }
            for (Entry<String, String> renameEntry : renameMap.entrySet())
            {
                propsRef.setProp(
                    InternalApiConsts.KEY_RENAME_STORPOOL_MAP + ReadOnlyProps.PATH_SEPARATOR + renameEntry.getKey(),
                    renameEntry.getValue(),
                    namespace
                );
            }
            if (add && forceRestore)
            {
                propsRef.setProp(
                    InternalApiConsts.KEY_FORCE_RESTORE,
                    ApiConsts.VAL_TRUE,
                    namespace
                );
            }
            if (dstRscNameRef != null && !dstRscNameRef.isEmpty())
            {
                response.add(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.WARN_INVLD_CONF,
                        "target resource definition is only usable in combiantion with --rd, but not --rg. " +
                            "Ignoring target resource definition parameter!"
                    )
                );
            }
            rscDfnsToCheck.addAll(rscGrp.getRscDfns());
        }
        else
        {
            systemConfRepository.setCtrlProp(
                InternalApiConsts.KEY_TRIPLE_ENABLED,
                add ? ApiConsts.VAL_TRUE : ApiConsts.VAL_FALSE,
                namespace
            );
            if (nodeNameRef != null && !nodeNameRef.isEmpty())
            {
                systemConfRepository.setCtrlProp(
                    InternalApiConsts.KEY_SCHEDULE_PREF_NODE,
                    nodeNameRef,
                    namespace
                );
            }
            for (Entry<String, String> renameEntry : renameMap.entrySet())
            {
                systemConfRepository.setCtrlProp(
                    InternalApiConsts.KEY_RENAME_STORPOOL_MAP + ReadOnlyProps.PATH_SEPARATOR + renameEntry.getKey(),
                    renameEntry.getValue(),
                    namespace
                );
            }
            if (add && forceRestore)
            {
                systemConfRepository.setCtrlProp(
                    InternalApiConsts.KEY_FORCE_RESTORE,
                    ApiConsts.VAL_TRUE,
                    namespace
                );
            }
            if (dstRscNameRef != null && !dstRscNameRef.isEmpty())
            {
                response.add(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.WARN_INVLD_CONF,
                        "target resource definition is only usable in combiantion with --rd, " +
                            "but not on controller level. Ignoring target resource definition parameter!"
                    )
                );
            }
            rscDfnsToCheck.addAll(rscDfnRepo.getMapForView().values());
        }
        ctrlTransactionHelper.commit();
        addOrRemoveTasks(schedule, remote, rscDfnsToCheck);
        // no update stlt, since schedule information is of no concern to the stlts
        response.addEntry(
            ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.MODIFIED,
                    buildSuccessMessage(
                        scheduleNameRef,
                        (add ? "enabled" : "disabled"),
                        rscNameRef,
                        grpNameRef,
                        remoteNameRef
                    )
                )
                .build()
        );
        return Flux.<ApiCallRc>just(response);
    }

    private String buildSuccessMessage(
        String scheduleNameRef,
        String actionRef,
        @Nullable String rscNameRef,
        @Nullable String rscGrpNameRef,
        String remoteNameRef
    )
    {
        StringBuilder sb = new StringBuilder("Backup shipping schedule '")
            .append(scheduleNameRef)
            .append("' successfully ")
            .append(actionRef);
        if (rscNameRef != null)
        {
            sb.append(" for resource definition '").append(rscNameRef).append("'");
        }
        else if (rscGrpNameRef != null)
        {
            sb.append(" for resource group '").append(rscGrpNameRef).append("'");
        }
        else
        {
            sb.append(" on controller");
        }
        sb.append(" to remote '")
            .append(remoteNameRef)
            .append("'.");
        return sb.toString();
    }

    /**
     * Starts or stops the scheduled tasks depending on the prio-props-result
     */
    private void addOrRemoveTasks(Schedule schedule, AbsRemote remote, List<ResourceDefinition> rscDfnsToCheck)
    {
        ReadOnlyProps ctrlProps = systemConfRepository.getCtrlConfForView();
        for (ResourceDefinition rscDfn : rscDfnsToCheck)
        {
            PriorityProps prioProps = new PriorityProps(
                rscDfn.getProps(),
                rscDfn.getResourceGroup().getProps(),
                ctrlProps
            );
            String prop = prioProps.getProp(
                remote.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    schedule.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    InternalApiConsts.KEY_TRIPLE_ENABLED,
                InternalApiConsts.NAMESPC_SCHEDULE
            );
            if (prop != null && Boolean.parseBoolean(prop))
            {
                if (rscDfn.getResourceCount() > 0)
                {
                    scheduleService.get().addNewTask(rscDfn, schedule, remote, false);
                }
                else
                {
                    errorReporter.logDebug(
                        "ResourceDefinition %s to remote %s scheduled, but will not ship unless resources are created.",
                        rscDfn.getName(),
                        remote.getName()
                    );
                }
            }
            else
            {
                scheduleService.get().removeSingleTask(schedule, remote, rscDfn);
            }
        }
    }

    /**
     * Called by the client to delete a remote-schedule-pair on rscDfn, rscGrp, or ctrl level.
     */
    public Flux<ApiCallRc> deleteSchedule(
        String rscNameRef,
        String grpNameRef,
        String remoteNameRef,
        String scheduleNameRef
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Delete backup schedule",
            lockGuardFactory.create().write(LockObj.RSC_DFN_MAP, LockObj.RSC_GRP_MAP, LockObj.CTRL_CONFIG)
                .read(LockObj.REMOTE_MAP, LockObj.SCHEDULE_MAP)
                .buildDeferred(),
            () -> deleteScheduleInTransaction(rscNameRef, grpNameRef, remoteNameRef, scheduleNameRef)
        );
    }

    /**
     * Removes the correct props to delete the remote-schedule-pair from rscDfn, rscGrp, or ctrl-level
     */
    private Flux<ApiCallRc> deleteScheduleInTransaction(
        String rscNameRef,
        String grpNameRef,
        String remoteNameRef,
        String scheduleNameRef
    ) throws InvalidKeyException, DatabaseException, InvalidValueException
    {
        AbsRemote remote = ctrlApiDataLoader.loadRemote(remoteNameRef);
        Schedule schedule = ctrlApiDataLoader.loadSchedule(scheduleNameRef);
        List<ResourceDefinition> rscDfnsToCheck = new ArrayList<>();
        if (rscNameRef != null && !rscNameRef.isEmpty())
        {
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef);
            Props propsRef = rscDfn.getProps();
            propsRef.removeProp(
                InternalApiConsts.NAMESPC_SCHEDULE + ReadOnlyProps.PATH_SEPARATOR +
                    remote.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    schedule.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    InternalApiConsts.KEY_TRIPLE_ENABLED
            );
            propsRef.removeProp(
                InternalApiConsts.NAMESPC_SCHEDULE + ReadOnlyProps.PATH_SEPARATOR +
                    remote.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    schedule.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    InternalApiConsts.KEY_SCHEDULE_PREF_NODE
            );
            rscDfnsToCheck.add(rscDfn);
        }
        else if (grpNameRef != null && !grpNameRef.isEmpty())
        {
            ResourceGroup rscGrp = ctrlApiDataLoader.loadResourceGroup(grpNameRef);
            Props propsRef = rscGrp.getProps();
            propsRef.removeProp(
                InternalApiConsts.NAMESPC_SCHEDULE + ReadOnlyProps.PATH_SEPARATOR +
                    remote.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    schedule.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    InternalApiConsts.KEY_TRIPLE_ENABLED
            );
            propsRef.removeProp(
                InternalApiConsts.NAMESPC_SCHEDULE + ReadOnlyProps.PATH_SEPARATOR +
                    remote.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    schedule.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    InternalApiConsts.KEY_SCHEDULE_PREF_NODE
            );
            rscDfnsToCheck.addAll(rscGrp.getRscDfns());
        }
        else
        {
            systemConfRepository.removeCtrlProp(
                remote.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    schedule.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    InternalApiConsts.KEY_TRIPLE_ENABLED,
                InternalApiConsts.NAMESPC_SCHEDULE
            );
            systemConfRepository.removeCtrlProp(
                remote.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    schedule.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR +
                    InternalApiConsts.KEY_SCHEDULE_PREF_NODE,
                InternalApiConsts.NAMESPC_SCHEDULE
            );
            rscDfnsToCheck.addAll(rscDfnRepo.getMapForView().values());
        }
        ctrlTransactionHelper.commit();
        addOrRemoveTasks(schedule, remote, rscDfnsToCheck);
        // no update stlt, since schedule information is of no concern to the stlts
        ApiCallRcImpl response = new ApiCallRcImpl();
        response.addEntry(
            ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.DELETED,
                    buildSuccessMessage(scheduleNameRef, "deleted", rscNameRef, grpNameRef, remoteNameRef)
                )
                .build()
        );
        return Flux.<ApiCallRc>just(response);
    }
}
