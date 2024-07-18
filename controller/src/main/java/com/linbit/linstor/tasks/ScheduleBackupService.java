package com.linbit.linstor.tasks;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackgroundRunner;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupCreateApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Schedule;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.ScheduleRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.TaskScheduleService.Task;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.cronutils.model.time.ExecutionTime;
import com.google.inject.Provider;
import reactor.core.publisher.Flux;

@Singleton
public class ScheduleBackupService implements SystemService
{
    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "Schedule backup service";
    private static final long NOT_STARTED_YET = -1;

    static
    {
        try
        {
            SERVICE_NAME = new ServiceName("ScheduleBackupService");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                String.format(
                    "%s class contains an invalid name constant",
                    ScheduleBackupService.class.getName()
                ),
                nameExc
            );
        }
    }

    private final TaskScheduleService taskScheduleService;
    private final ResourceDefinitionRepository rscDfnRepo;
    private final AccessContext sysCtx;
    private final SystemConfRepository systemConfRepository;
    private final RemoteRepository remoteRepo;
    private final ScheduleRepository scheduleRepo;
    private final LockGuardFactory lockGuardFactory;
    private final ErrorReporter errorReporter;
    private final Provider<CtrlBackupCreateApiCallHandler> backupCrtApiCallHandler;
    private final Provider<CtrlBackupL2LSrcApiCallHandler> backupL2LSrcApiCallHandler;
    private final ScopeRunner scopeRunner;
    private final BackgroundRunner backgroundRunner;

    private final Object syncObj = new Object();
    private final Map<Schedule, Set<ScheduledShippingConfig>> scheduleLookupMap = new TreeMap<>();
    private final Map<ResourceDefinition, Set<ScheduledShippingConfig>> rscDfnLookupMap = new TreeMap<>();
    private final Map<AbsRemote, Set<ScheduledShippingConfig>> remoteLookupMap = new TreeMap<>();
    // so far this is only used to list all scheduled shippings, running or waiting
    // as well as to count retries
    // DO NOT try to do anything with the task in these config-objects, as it most likely does not exist anymore
    private final Set<ScheduledShippingConfig> activeShippings = new TreeSet<>();

    private ServiceName serviceInstanceName;
    private boolean running = false;

    @Inject
    public ScheduleBackupService(
        TaskScheduleService taskScheduleServiceRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        @SystemContext AccessContext sysCtxRef,
        SystemConfRepository systemConfRepositoryRef,
        RemoteRepository remoteRepoRef,
        ScheduleRepository scheduleRepoRef,
        LockGuardFactory lockGuardFactoryRef,
        ErrorReporter errorReporterRef,
        Provider<CtrlBackupCreateApiCallHandler> backupCrtApiCallHandlerRef,
        Provider<CtrlBackupL2LSrcApiCallHandler> backupL2LSrcApiCallHandlerRef,
        ScopeRunner scopeRunnerRef,
        BackgroundRunner backgroundRunnerRef
    )
    {
        taskScheduleService = taskScheduleServiceRef;
        rscDfnRepo = rscDfnRepoRef;
        sysCtx = sysCtxRef;
        systemConfRepository = systemConfRepositoryRef;
        remoteRepo = remoteRepoRef;
        scheduleRepo = scheduleRepoRef;
        lockGuardFactory = lockGuardFactoryRef;
        errorReporter = errorReporterRef;
        backupCrtApiCallHandler = backupCrtApiCallHandlerRef;
        backupL2LSrcApiCallHandler = backupL2LSrcApiCallHandlerRef;
        scopeRunner = scopeRunnerRef;
        backgroundRunner = backgroundRunnerRef;
        serviceInstanceName = SERVICE_NAME;
    }

    @Override
    public ServiceName getServiceName()
    {
        return SERVICE_NAME;
    }

    @Override
    public String getServiceInfo()
    {
        return SERVICE_INFO;
    }

    @Override
    public ServiceName getInstanceName()
    {
        return serviceInstanceName;
    }

    @Override
    public boolean isStarted()
    {
        return running;
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceName)
    {
        if (instanceName == null)
        {
            serviceInstanceName = SERVICE_NAME;
        }
        else
        {
            serviceInstanceName = instanceName;
        }
    }

    @Override
    public void start() throws SystemServiceStartException
    {
        running = true;
        try
        {
            for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView(sysCtx).values())
            {
                addAllTasks(rscDfn, sysCtx);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public void shutdown()
    {
        running = false;
        synchronized (syncObj)
        {
            // only go through one map, since they have the same values, just different key-objects
            for (Set<ScheduledShippingConfig> configSet : scheduleLookupMap.values())
            {
                for (ScheduledShippingConfig config : configSet)
                {
                    taskScheduleService.rescheduleAt(config.task, Task.END_TASK);
                }
            }
            scheduleLookupMap.clear();
            remoteLookupMap.clear();
            rscDfnLookupMap.clear();
        }
    }

    @Override
    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        // no-op
    }

    public Set<ScheduledShippingConfig> getAllFilteredActiveShippings(
        String rscName,
        String remoteName,
        String scheduleName
    )
    {
        synchronized (syncObj)
        {
            Set<ScheduledShippingConfig> ret = new TreeSet<>();
            for (ScheduledShippingConfig conf : activeShippings)
            {
                boolean add = rscName == null || rscName.isEmpty() ||
                    conf.rscDfn.getName().value.equalsIgnoreCase(rscName);
                add &= remoteName == null || remoteName.isEmpty() ||
                    conf.remote.getName().value.equalsIgnoreCase(remoteName);
                add &= scheduleName == null || scheduleName.isEmpty() ||
                    conf.schedule.getName().value.equalsIgnoreCase(scheduleName);

                if (add)
                {
                    ret.add(conf);
                }
            }
            return ret;
        }
    }

    public ApiCallRc addAllTasks(ResourceDefinition rscDfn, AccessContext accCtx) throws AccessDeniedException
    {
        ApiCallRcImpl rc = new ApiCallRcImpl();
        try (LockGuard lg = lockGuardFactory.build(LockType.READ, LockObj.RSC_DFN_MAP, LockObj.RSC_GRP_MAP))
        {
            PriorityProps prioProps = new PriorityProps(
                rscDfn.getProps(accCtx),
                rscDfn.getResourceGroup().getProps(accCtx),
                systemConfRepository.getCtrlConfForView(accCtx)
            );
            for (
                Entry<String, String> prop : prioProps.renderRelativeMap(InternalApiConsts.NAMESPC_SCHEDULE).entrySet()
            )
            {
                String[] keyParts = prop.getKey().split(ReadOnlyProps.PATH_SEPARATOR);
                // key for activating scheduled shipping is {remoteName}/{scheduleName}/Enabled
                if (
                    keyParts.length == 3 && keyParts[2].equals(InternalApiConsts.KEY_TRIPLE_ENABLED) &&
                        prop.getValue().equals(ApiConsts.VAL_TRUE)
                )
                {
                    String remoteStr = keyParts[0];
                    String scheduleStr = keyParts[1];
                    AbsRemote remote = remoteRepo.get(accCtx, new RemoteName(remoteStr));
                    Schedule schedule = scheduleRepo.get(accCtx, new ScheduleName(scheduleStr));
                    addNewTask(rscDfn, schedule, remote, false, accCtx);
                    if (remote == null || schedule == null)
                    {
                        rc.addEntry(
                            String.format(
                                "Resource Definition %s was not set to be shipped " +
                                    "because %s %s has already been deleted.",
                                rscDfn.getName().displayValue,
                                remote == null ? "remote" : "schedule",
                                remote == null ? remoteStr : scheduleStr
                            ),
                            ApiConsts.MASK_SCHEDULE | ApiConsts.INFO_NOOP
                        );
                    }
                    else
                    {
                        rc.addEntry(
                            "Resource Definition " + rscDfn.getName().displayValue +
                                " sucessfully set to be shipped per schedule " + scheduleStr +
                                " to remote " + remoteStr + ".",
                            ApiConsts.MASK_SCHEDULE | ApiConsts.CREATED
                        );
                    }
                }
            }
            return rc;
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public Flux<ApiCallRc> fluxAllNewTasks(ResourceDefinition rscDfn, AccessContext accCtx)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Add new backup-schedule task",
                lockGuardFactory.buildDeferred(
                    LockType.READ, LockObj.RSC_DFN_MAP, LockObj.RSC_GRP_MAP
                ),
                () -> {
                    ApiCallRc rc = addAllTasks(rscDfn, accCtx);
                    return Flux.just(rc);
                }
            );
    }

    /**
     * Add a rscDfn to be scheduled for automated backup shipping.
     * Only call this method if there are no recent automated backups of the given rscDfn-schedule-remote combination
     *
     * @param rscDfn
     * @param schedule
     * @param remote
     * @param accCtx
     *
     * @throws AccessDeniedException
     */
    public void addNewTask(
        ResourceDefinition rscDfn,
        Schedule schedule,
        AbsRemote remote,
        boolean lastInc,
        AccessContext accCtx
    )
        throws AccessDeniedException
    {
        addTaskAgain(rscDfn, schedule, remote, NOT_STARTED_YET, true, false, lastInc, accCtx);
    }

    /**
     * Add a rscDfn to be scheduled for automated backup shipping.
     * Use this method if an automated shipping just finished and the rscDfn needs to be re-scheduled
     * This method assumes that currently there is no task for this rscDfn-schedule-remote triplet and will therefore
     * create a new task.
     * DO NOT set lastStartTime to a negative number - if this seems necessary for any reason, use addNewTask(...)
     * instead
     *
     * @param rscDfn
     * @param schedule
     * @param remote
     * @param lastStartTime
     * @param accCtx
     *
     * @throws AccessDeniedException
     */
    public void addTaskAgain(
        ResourceDefinition rscDfn,
        Schedule schedule,
        AbsRemote remote,
        long lastStartTime,
        boolean lastBackupSucceeded,
        boolean forceSkip,
        boolean lastInc,
        AccessContext accCtx
    ) throws AccessDeniedException
    {
        if (
            running &&
                schedule != null && remote != null && rscDfn != null &&
                rscDfn.getNotDeletedDiskfulCount(accCtx) > 0
        )
        {
            boolean isNew = true;
            synchronized (syncObj)
            {
                // only check in one map, since they have the same values, just different key-objects
                Set<ScheduledShippingConfig> configSet = scheduleLookupMap.get(schedule);
                if (configSet != null)
                {
                    for (ScheduledShippingConfig config : configSet)
                    {
                        if (config.rscDfn.equals(rscDfn) && config.remote.equals(remote))
                        {
                            isNew = false;
                            break;
                        }
                    }
                }
            }
            if (isNew)
            {
                String prefNode = null;
                Map<String, String> renameStorpoolMap = new HashMap<>();
                boolean forceRestore = false;
                try (LockGuard lg = lockGuardFactory.build(LockType.READ, LockObj.RSC_DFN_MAP, LockObj.RSC_GRP_MAP))
                {
                    PriorityProps prioProps = new PriorityProps(
                        rscDfn.getProps(accCtx),
                        rscDfn.getResourceGroup().getProps(accCtx),
                        systemConfRepository.getCtrlConfForView(accCtx)
                    );
                    // namespace for schedule-remote-rsc props is {remoteName}/{scheduleName}/
                    String namespace = InternalApiConsts.NAMESPC_SCHEDULE + ReadOnlyProps.PATH_SEPARATOR +
                        remote.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR + schedule.getName().displayValue +
                        ReadOnlyProps.PATH_SEPARATOR;
                    Map<String, String> propsInNamespace = prioProps.renderRelativeMap(
                        namespace + InternalApiConsts.KEY_RENAME_STORPOOL_MAP
                    );
                    for (Entry<String, String> prop : propsInNamespace.entrySet())
                    {
                        renameStorpoolMap.put(prop.getKey(), prop.getValue());
                    }
                    prefNode = prioProps.getProp(
                        InternalApiConsts.KEY_SCHEDULE_PREF_NODE,
                        namespace
                    );
                    // key for prefNode is {remoteName}/{scheduleName}/KEY_FORCE_RESTORE
                    forceRestore = Boolean.parseBoolean(
                        prioProps.getProp(
                            remote.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR + schedule.getName().displayValue +
                                ReadOnlyProps.PATH_SEPARATOR + InternalApiConsts.KEY_FORCE_RESTORE,
                            InternalApiConsts.NAMESPC_SCHEDULE
                        )
                    );
                }
                ZonedDateTime now = ZonedDateTime.now();
                Pair<Long, Boolean> infoPair = getTimeoutAndType(
                    schedule,
                    accCtx,
                    now,
                    lastStartTime,
                    lastBackupSucceeded,
                    forceSkip || tooManyRetries(lastBackupSucceeded, schedule, accCtx),
                    lastInc
                );
                boolean incremental = infoPair.objB;
                Long timeout = infoPair.objA;
                errorReporter.logDebug(
                    "ResourceDefinition %s to remote %s scheduled to ship %s backup in %d milliseconds.",
                    rscDfn.getName(),
                    remote.getName(),
                    incremental ? "incremental" : "full",
                    timeout
                );

                ScheduledShippingConfig config = new ScheduledShippingConfig(
                    schedule,
                    remote,
                    rscDfn,
                    lastInc,
                    infoPair,
                    now.toEpochSecond() * 1000,
                    renameStorpoolMap
                );
                boolean confIsActive = activeShippings.contains(config);
                if (lastStartTime >= 0 && confIsActive || lastStartTime == NOT_STARTED_YET)
                {
                    BackupShippingTask task = new BackupShippingTask(
                        new BackupShippingtaskConfig(
                            accCtx,
                            config,
                            rscDfn.getName().displayValue,
                            prefNode,
                            incremental,
                            forceRestore,
                            lastStartTime
                        )
                    );
                    config.task = task;
                    synchronized (syncObj)
                    {
                        rscDfnLookupMap.computeIfAbsent(rscDfn, ignored -> new HashSet<>()).add(config);
                        scheduleLookupMap.computeIfAbsent(schedule, ignored -> new HashSet<>()).add(config);
                        remoteLookupMap.computeIfAbsent(remote, ignored -> new HashSet<>()).add(config);
                    }
                    if (timeout != null)
                    {
                        // it should not be possible for timeout to be null here...
                        if (timeout == 0)
                        {
                            taskScheduleService.addTask(task);
                        }
                        else
                        {
                            taskScheduleService.rescheduleAt(task, timeout);
                        }
                        if (!confIsActive)
                        {
                            // DO NOT overwrite with a new config if an entry already exists!
                            activeShippings.add(config);
                        }
                    }
                }
            }
        }
        // maybe add else if running ...
    }

    public ScheduledShippingConfig getConfig(ResourceDefinition rscDfn, AbsRemote remote, Schedule schedule)
    {
        ScheduledShippingConfig ret = null;
        Set<ScheduledShippingConfig> confs = rscDfnLookupMap.get(rscDfn);
        if (confs != null)
        {
            for (ScheduledShippingConfig conf : confs)
            {
                if (conf.schedule.equals(schedule) && conf.remote.equals(remote))
                {
                    ret = conf;
                }
            }
        }
        return ret;
    }

    /**
     * Unconditionally reschedules all tasks that use the given schedule.
     *
     * @param scheduleRef
     *
     * @throws AccessDeniedException
     */
    public void modifyTasks(Schedule scheduleRef, AccessContext accCtx) throws AccessDeniedException
    {
        if (running)
        {
            synchronized (syncObj)
            {
                // only check in one map, since they have the same values, just different key-objects
                Set<ScheduledShippingConfig> configSet = scheduleLookupMap.get(scheduleRef);
                if (configSet != null)
                {
                    ZonedDateTime now = ZonedDateTime.now();
                    for (ScheduledShippingConfig config : configSet)
                    {
                        Pair<Long, Boolean> infoPair = getTimeoutAndType(
                            scheduleRef, accCtx, now, config.task.getPreviousTaskStartTime(), true, false,
                            config.lastInc
                        );
                        config.task.setIncremental(infoPair.objB);
                        for (ScheduledShippingConfig conf : activeShippings)
                        {
                            if (conf.equals(config))
                            {
                                conf.timeoutAndType = infoPair;
                                conf.timeoutAndTypeCalculatedFrom = now.toEpochSecond() * 1000;
                            }
                        }
                        taskScheduleService.rescheduleAt(config.task, infoPair.objA);
                    }
                }
            }
        }
    }

    private boolean tooManyRetries(boolean lastBackupSucceeded, Schedule schedule, AccessContext accCtx)
        throws AccessDeniedException
    {
        boolean ret = false;
        if (!lastBackupSucceeded && schedule.getOnFailure(accCtx).equals(Schedule.OnFailure.RETRY))
        {
            // the schedule has already been run at least once, otherwise lastBackupSucceeded couldn't be false
            for (ScheduledShippingConfig conf : activeShippings)
            {
                if (conf.schedule.equals(schedule))
                {
                    Integer maxRetries = schedule.getMaxRetries(accCtx);
                    if (maxRetries != null)
                    {
                        if (maxRetries <= conf.retryCt)
                        {
                            errorReporter.logWarning(
                                "Shipping of resource %s to remote %s has already been retried %d time(s)." +
                                    " Retry attempts stopped.",
                                conf.rscDfn.getName().displayValue,
                                conf.remote.getName().displayValue,
                                conf.retryCt
                            );
                            conf.retryCt = 0;
                            ret = true;
                        }
                        else
                        {
                            conf.retryCt++;
                            errorReporter.logWarning(
                                "Shipping of resource %s to remote %s will be retried for the %d. time." +
                                    " Max retries: %d",
                                conf.rscDfn.getName().displayValue,
                                conf.remote.getName().displayValue,
                                conf.retryCt,
                                maxRetries
                            );
                        }
                    }
                    else
                    {
                        errorReporter.logWarning(
                            "Shipping of resource %s to remote %s will be retried. Max retries: infinite",
                            conf.rscDfn.getName().displayValue,
                            conf.remote.getName().displayValue
                        );
                    }
                    break;
                }
            }
        }
        return ret;
    }

    static Pair<Long, Boolean> getTimeoutAndType(
        Schedule schedule,
        AccessContext accCtx,
        ZonedDateTime now,
        long lastStartTime,
        boolean lastBackupSucceeded,
        boolean forceSkip,
        boolean lastIncr
    )
        throws AccessDeniedException
    {
        boolean incExists = schedule.getIncCron(accCtx) != null;
        // skip: whether we retry or not; forceSkip: always skip
        boolean skip = schedule.getOnFailure(accCtx).equals(Schedule.OnFailure.SKIP) || forceSkip;
        ExecutionTime fullExec = ExecutionTime.forCron(schedule.getFullCron(accCtx));
        ExecutionTime incrExec = null;
        if (incExists)
        {
            incrExec = ExecutionTime.forCron(schedule.getIncCron(accCtx));
        }

        ZonedDateTime nextFullFromNow = nextExec(fullExec, now);
        ZonedDateTime nextIncrFromNow;
        if (incExists)
        {
            nextIncrFromNow = nextExec(incrExec, now);
        }
        else
        {
            // when there is no cron-definition for incremental backups, make sure nextIncrFromNow can never be before
            // nextFullFromNow. Use plusYears to make sure no rounding can undo this change
            nextIncrFromNow = nextFullFromNow.plusYears(1);
        }
        long nowMillis = now.toEpochSecond() * 1000;
        long timeout;
        boolean incr;
        if (lastStartTime == NOT_STARTED_YET)
        {
            Pair<Long, Boolean> result = earlier(nextFullFromNow, nextIncrFromNow, nowMillis);
            timeout = result.objA;
            incr = result.objB;
        }
        else
        {
            ZonedDateTime lastStart = ZonedDateTime
                .ofInstant(Instant.ofEpochMilli(lastStartTime), ZoneId.systemDefault());
            ZonedDateTime lastFullExecFromNow = lastExec(fullExec, now);
            ZonedDateTime lastFullExecFromLastStart = lastExec(fullExec, lastStart);
            ZonedDateTime nextIncrExecFromLastStart;
            if (incExists)
            {
                nextIncrExecFromLastStart = nextExec(incrExec, lastStart);
            }
            else
            {
                // when there is no cron-definition for incremental backups, nextIncrExecFromLastStart needs to be not
                // equal to nextIncrFromNow. Use plusYears to make sure no rounding can undo this change
                nextIncrExecFromLastStart = nextIncrFromNow.plusYears(1);
            }
            if (lastFullExecFromNow.isEqual(lastFullExecFromLastStart) || (!lastBackupSucceeded && skip))
            {
                if (nextIncrFromNow.isEqual(nextIncrExecFromLastStart) || !incExists || (!lastBackupSucceeded && skip))
                {
                    // we are on schedule or we do not have an incremental schedule
                    Pair<Long, Boolean> result = earlier(nextFullFromNow, nextIncrFromNow, nowMillis);
                    timeout = result.objA;
                    incr = result.objB;
                }
                else
                {
                    // we missed an incremental. start asap
                    // could also be that now == nextIncrExecFromLastStart, but then we also want to start asap
                    timeout = 0;
                    incr = true;
                }
            }
            else
            {
                // we missed a full. start asap
                timeout = 0;
                incr = false;
            }
            if (!lastBackupSucceeded && !skip)
            {
                timeout = 60000L;
                incr = incr && lastIncr;
            }
        }
        return new Pair<>(timeout, incr);
    }

    private static Pair<Long, Boolean> earlier(ZonedDateTime nextFullRef, ZonedDateTime nextIncrRef, long nowRef)
    {
        long timeout;
        boolean incr;
        if (nextIncrRef.isBefore(nextFullRef))
        {
            timeout = nextIncrRef.toEpochSecond() * 1000 + nextIncrRef.getNano() / 1_000_000 - nowRef;
            incr = true;
        }
        else
        {
            timeout = nextFullRef.toEpochSecond() * 1000 + nextFullRef.getNano() / 1_000_000 - nowRef;
            incr = false;
        }
        return new Pair<>(timeout, incr);
    }

    public void removeTasks(ResourceDefinition rscDfnRef)
    {
        synchronized (syncObj)
        {
            /*
             * we need to get the set for removeFromAll() from the activeShippings set, since it is the only place where
             * all active schedule-configs can be found
             */
            removeFromAll(getAllFilteredActiveShippings(rscDfnRef.getName().displayValue, null, null));
        }
    }

    public void removeTasks(Schedule scheduleRef, AccessContext accCtx)
        throws InvalidKeyException, AccessDeniedException, DatabaseException
    {
        synchronized (syncObj)
        {
            // see comment of removeTasks(rscDfn)
            removeFromAll(getAllFilteredActiveShippings(null, null, scheduleRef.getName().displayValue));
            removeAllRelatedProps(null, scheduleRef.getName().displayValue, accCtx);
        }
    }

    public void removeTasks(AbsRemote remoteRef, AccessContext accCtx)
        throws InvalidKeyException, AccessDeniedException, DatabaseException
    {
        synchronized (syncObj)
        {
            // see comment of removeTasks(rscDfn)
            removeFromAll(getAllFilteredActiveShippings(null, remoteRef.getName().displayValue, null));
            removeAllRelatedProps(remoteRef.getName().displayValue, null, accCtx);
        }
    }

    private void removeFromAll(Set<ScheduledShippingConfig> confsToRemove)
    {
        if (confsToRemove != null)
        {
            for (ScheduledShippingConfig conf : confsToRemove)
            {
                removeSingleTask(conf, false);
            }
        }
    }

    void removeSingleTask(ScheduledShippingConfig conf, boolean fromTask)
    {
        if (conf != null)
        {
            synchronized (syncObj)
            {
                if (!fromTask)
                {
                    activeShippings.remove(conf);
                    // find config with current task and reschedule with negative delay to cancel task
                    Set<ScheduledShippingConfig> confs = rscDfnLookupMap.get(conf.rscDfn);
                    if (confs != null)
                    {
                        for (ScheduledShippingConfig config : confs)
                        {
                            if (config.equals(conf))
                            {
                                taskScheduleService.rescheduleAt(config.task, Task.END_TASK);
                            }
                        }
                    }
                }
                removeFromSingleMap(remoteLookupMap, conf.remote, conf);
                removeFromSingleMap(scheduleLookupMap, conf.schedule, conf);
                removeFromSingleMap(rscDfnLookupMap, conf.rscDfn, conf);
            }
        }
    }

    public void removeSingleTask(Schedule schedule, AbsRemote remote, ResourceDefinition rscDfn)
    {
        removeSingleTask(new ScheduledShippingConfig(schedule, remote, rscDfn, false, new Pair<>(), null, null), false);
    }

    /**
     * Needs to be called inside a synchronized block, as well as have the following LockObjs in WRITE-lock:
     * RSC_DFN_MAP
     * RSC_GRP_MAP
     * CTRL_CONF
     *
     * @param remoteName
     *     can be null if scheduleName is set
     * @param scheduleName
     *     can be null if remoteName is set
     * @param accCtx
     *
     * @throws AccessDeniedException
     * @throws DatabaseException
     * @throws InvalidKeyException
     */
    private void removeAllRelatedProps(String remoteName, String scheduleName, AccessContext accCtx)
        throws AccessDeniedException, InvalidKeyException, DatabaseException
    {
        Set<ResourceGroup> rscGrps = new HashSet<>();
        for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView(accCtx).values())
        {
            rscGrps.add(rscDfn.getResourceGroup());
            removePropsFromContainer(rscDfn.getProps(accCtx), remoteName, scheduleName);
        }
        for (ResourceGroup rscGrp : rscGrps)
        {
            removePropsFromContainer(rscGrp.getProps(accCtx), remoteName, scheduleName);
        }
        removePropsFromContainer(systemConfRepository.getCtrlConfForChange(accCtx), remoteName, scheduleName);
    }

    private void removePropsFromContainer(Props props, String remoteName, String scheduleName)
        throws InvalidKeyException, DatabaseException, AccessDeniedException
    {
        Optional<Props> namespaceProps = props.getNamespace(InternalApiConsts.NAMESPC_SCHEDULE);
        if (namespaceProps.isPresent())
        {
            Set<String> copySet = new HashSet<>(namespaceProps.get().map().keySet());
            final int expectedKeyLength = 4;
            for (String propKey : copySet)
            {
                // key for activating scheduled shipping is namespace/{remoteName}/{scheduleName}/Enabled
                String[] splitKey = propKey.split(ReadOnlyProps.PATH_SEPARATOR);
                if (splitKey.length == expectedKeyLength && (
                    remoteName != null && splitKey[1].equals(remoteName) ||
                    scheduleName != null && splitKey[2].equals(scheduleName))
                )
                {
                    props.removeProp(propKey);
                }
            }
        }
    }

    private <T> void removeFromSingleMap(
        Map<T, Set<ScheduledShippingConfig>> lookupMap,
        T key,
        ScheduledShippingConfig conf
    )
    {
        Set<ScheduledShippingConfig> set = lookupMap.get(key);
        if (set != null)
        {
            set.remove(conf);
            if (set.isEmpty())
            {
                lookupMap.remove(key);
            }
        }
    }

    private static ZonedDateTime nextExec(ExecutionTime exec, ZonedDateTime zdt)
    {
        return exec(exec, zdt, true);
    }

    private static ZonedDateTime lastExec(ExecutionTime exec, ZonedDateTime zdt)
    {
        return exec(exec, zdt, false);
    }

    private static ZonedDateTime exec(ExecutionTime exec, ZonedDateTime zdt, boolean next)
    {
        ZonedDateTime ret;
        if (next)
        {
            ret = exec.nextExecution(zdt).get();
        }
        else
        {
            /*
             * DO NOT use exec.isMatch as that method truncates seconds in CRON_UNIX and CRON4J format
             */
            /*
             * In this else case we are only interested in the previous (last) execution point.
             * However, we have 2 cases: Either zdt is between two execution points or exactly on one exec point.
             * If we are exactly on one execution point, going to the last and afterwards to the next point will
             * result in where we started (zdt).
             * If zdt is between two exec points, we only need .lastExecution() (therefore the "rollback")
             */
            ZonedDateTime tmp = exec.lastExecution(zdt).get();
            ret = exec.nextExecution(tmp).get();
            if (!ret.equals(zdt))
            {
                ret = tmp;
            }
        }
        return ret;
    }

    public static class ScheduledShippingConfig implements Comparable<ScheduledShippingConfig>
    {
        public final Schedule schedule;
        public final AbsRemote remote;
        public final ResourceDefinition rscDfn;
        public final boolean lastInc;
        public final Map<String, String> storpoolRenameMap;

        public Pair<Long, Boolean> timeoutAndType;
        public Long timeoutAndTypeCalculatedFrom;
        private BackupShippingTask task;
        public int retryCt;

        ScheduledShippingConfig(
            Schedule scheduleRef,
            AbsRemote remoteRef,
            ResourceDefinition rscDfnRef,
            boolean lastIncRef,
            Pair<Long, Boolean> timeoutAndTypeRef,
            Long timeoutAndTypeCalculatedFromRef,
            Map<String, String> storpoolRenameMapRef
        )
        {
            schedule = scheduleRef;
            remote = remoteRef;
            rscDfn = rscDfnRef;
            lastInc = lastIncRef;
            timeoutAndType = timeoutAndTypeRef;
            storpoolRenameMap = storpoolRenameMapRef;
            retryCt = 0;
            timeoutAndTypeCalculatedFrom = timeoutAndTypeCalculatedFromRef;
        }

        @Override
        public int compareTo(ScheduledShippingConfig other)
        {
            int cmp = schedule.getName().compareTo(other.schedule.getName());
            if (cmp == 0)
            {
                cmp = remote.getName().compareTo(other.remote.getName());
                if (cmp == 0)
                {
                    cmp = rscDfn.getName().compareTo(other.rscDfn.getName());
                }
            }
            return cmp;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((remote == null) ? 0 : remote.getName().hashCode());
            result = prime * result + ((rscDfn == null) ? 0 : rscDfn.getName().hashCode());
            result = prime * result + ((schedule == null) ? 0 : schedule.getName().hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (getClass() != obj.getClass())
            {
                return false;
            }
            ScheduledShippingConfig other = (ScheduledShippingConfig) obj;
            if (remote == null)
            {
                if (other.remote != null)
                {
                    return false;
                }
            }
            else if (other.remote == null || !remote.getName().equals(other.remote.getName()))
            {
                return false;
            }
            if (rscDfn == null)
            {
                if (other.rscDfn != null)
                {
                    return false;
                }
            }
            else if (other.rscDfn == null || !rscDfn.getName().equals(other.rscDfn.getName()))
            {
                return false;
            }
            if (schedule == null)
            {
                if (other.schedule != null)
                {
                    return false;
                }
            }
            else if (other.schedule == null || !schedule.getName().equals(other.schedule.getName()))
            {
                return false;
            }
            return true;
        }
    }

    /**
     * A simple holder class that holds objects required for the BackupShippingTask or can access
     * ScheduleBackupService's commonly used fields as the *ApiCallHandlers
     *
     * This class is deliberately not static so we can create delegate-getters to the containing
     * {@link ScheduleBackupService} instance.
     */
    class BackupShippingtaskConfig
    {
        private final AccessContext accCtx;
        private final ScheduledShippingConfig schedShipCfg;
        private final String rscName;
        private final String nodeName;
        private final boolean incremental;
        private final boolean forceRestore;
        private final long lastStartTime;

        private BackupShippingtaskConfig(
            AccessContext accCtxRef,
            ScheduledShippingConfig schedShipCfgRef,
            String rscNameRef,
            String nodeNameRef,
            boolean incrementalRef,
            boolean forceRestoreRef,
            long lastStartTimeRef
        )
        {
            accCtx = accCtxRef;
            schedShipCfg = schedShipCfgRef;
            rscName = rscNameRef;
            nodeName = nodeNameRef;
            incremental = incrementalRef;
            forceRestore = forceRestoreRef;
            lastStartTime = lastStartTimeRef;
        }

        AccessContext getAccCtx()
        {
            return accCtx;
        }

        ScheduledShippingConfig getSchedShipCfg()
        {
            return schedShipCfg;
        }

        String getRscName()
        {
            return rscName;
        }

        String getNodeName()
        {
            return nodeName;
        }

        boolean isIncremental()
        {
            return incremental;
        }

        boolean isForceRestore()
        {
            return forceRestore;
        }

        long getLastStartTime()
        {
            return lastStartTime;
        }

        ErrorReporter getErrorReporter()
        {
            return ScheduleBackupService.this.errorReporter;
        }

        CtrlBackupCreateApiCallHandler getBackupCreateApiCallHandler()
        {
            return ScheduleBackupService.this.backupCrtApiCallHandler.get();
        }

        CtrlBackupL2LSrcApiCallHandler getBackupL2LSrcApiCallHandler()
        {
            return ScheduleBackupService.this.backupL2LSrcApiCallHandler.get();
        }

        ScheduleBackupService getScheduleBackupService()
        {
            return ScheduleBackupService.this;
        }

        BackgroundRunner getBackgroundRunner()
        {
            return ScheduleBackupService.this.backgroundRunner;
        }

        LockGuardFactory getLockGuardFactory()
        {
            return ScheduleBackupService.this.lockGuardFactory;
        }

        ResourceDefinitionRepository getRscDfnRepo()
        {
            return ScheduleBackupService.this.rscDfnRepo;
        }
    }
}
