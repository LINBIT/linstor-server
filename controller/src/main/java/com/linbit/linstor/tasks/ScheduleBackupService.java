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
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlBackupApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlBackupL2LSrcApiCallHandler;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.objects.Remote;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Schedule;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.ScheduleRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
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
    private final Provider<CtrlBackupApiCallHandler> backupApiCallHandler;
    private final Provider<CtrlBackupL2LSrcApiCallHandler> backupL2LSrcApiCallHandler;
    private final ScopeRunner scopeRunner;

    private final Object syncObj = new Object();

    private ServiceName serviceInstanceName;
    private boolean running = false;
    private Map<Schedule, Set<ScheduledShippingConfig>> scheduleLookupMap = new TreeMap<>();
    private Map<ResourceDefinition, Set<ScheduledShippingConfig>> rscDfnLookupMap = new TreeMap<>();
    private Map<Remote, Set<ScheduledShippingConfig>> remoteLookupMap = new TreeMap<>();
    // so far this is only used to list all scheduled shippings, running or waiting
    // DO NOT try to do anything with the task in these config-objects, as it most likely does not exist anymore
    private Set<ScheduledShippingConfig> activeShippings = new TreeSet<>();

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
        Provider<CtrlBackupApiCallHandler> backupApiCallHandlerRef,
        Provider<CtrlBackupL2LSrcApiCallHandler> backupL2LSrcApiCallHandlerRef,
        ScopeRunner scopeRunnerRef
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
        backupApiCallHandler = backupApiCallHandlerRef;
        backupL2LSrcApiCallHandler = backupL2LSrcApiCallHandlerRef;
        scopeRunner = scopeRunnerRef;
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
                boolean add = rscName != null && !rscName.isEmpty() ||
                    conf.rscDfn.getName().value.equalsIgnoreCase(rscName);
                add |= remoteName != null && !remoteName.isEmpty() ||
                    conf.remote.getName().value.equalsIgnoreCase(remoteName);
                add |= scheduleName != null && !scheduleName.isEmpty() ||
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
                String[] keyParts = prop.getKey().split(Props.PATH_SEPARATOR);
                // key for activating scheduled shipping is {remoteName}/{scheduleName}/Enabled
                if (
                    keyParts.length == 3 && keyParts[2].equals(InternalApiConsts.KEY_TRIPLE_ENABLED) &&
                        prop.getValue().equals(ApiConsts.VAL_TRUE)
                )
                {
                    Remote remote = remoteRepo.get(accCtx, new RemoteName(keyParts[0]));
                    Schedule schedule = scheduleRepo.get(accCtx, new ScheduleName(keyParts[1]));
                    addNewTask(rscDfn, schedule, remote, false, accCtx);
                    rc.addEntry(
                        "Resource Definition " + rscDfn.getName().displayValue +
                            " sucessfully set to be shipped per schedule " + schedule.getName().displayValue +
                            " to remote " + remote.getName().displayValue + ".",
                        ApiConsts.MASK_SCHEDULE | ApiConsts.CREATED
                    );
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
        Remote remote,
        boolean lastInc,
        AccessContext accCtx
    )
        throws AccessDeniedException
    {
        addTaskAgain(rscDfn, schedule, remote, NOT_STARTED_YET, false, false, lastInc, accCtx);
    }

    /**
     * Add a rscDfn to be scheduled for automated backup shipping.
     * Use this method if an automated shipping just finished and the rscDfn needs to be re-scheduled
     * DO NOT set lastStartTime to a negative number - if this seems neccessary for any reason, use addNewTask(...)
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
        Remote remote,
        long lastStartTime,
        boolean lastBackupSucceeded,
        boolean forceSkip,
        boolean lastInc,
        AccessContext accCtx
    ) throws AccessDeniedException
    {
        if (running && schedule != null && remote != null && rscDfn != null)
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
                try (LockGuard lg = lockGuardFactory.build(LockType.READ, LockObj.RSC_DFN_MAP, LockObj.RSC_GRP_MAP))
                {
                    PriorityProps prioProps = new PriorityProps(
                        rscDfn.getProps(accCtx),
                        rscDfn.getResourceGroup().getProps(accCtx),
                        systemConfRepository.getCtrlConfForView(accCtx)
                    );
                    // key for prefNode is {remoteName}/{scheduleName}/KEY_SCHEDULE_PREF_NODE
                    prefNode = prioProps.getProp(
                        remote.getName().displayValue + Props.PATH_SEPARATOR + schedule.getName().displayValue
                            + Props.PATH_SEPARATOR + InternalApiConsts.KEY_SCHEDULE_PREF_NODE,
                        InternalApiConsts.NAMESPC_SCHEDULE
                    );
                }
                ZonedDateTime now = ZonedDateTime.now();
                Pair<Long, Boolean> infoPair = getTimeoutAndType(
                    schedule, accCtx, now, lastStartTime, lastBackupSucceeded, forceSkip, lastInc
                );
                boolean incremental = infoPair.objB;
                Long timeout = infoPair.objA;


                ScheduledShippingConfig config = new ScheduledShippingConfig(
                    schedule,
                    remote,
                    rscDfn,
                    lastInc,
                    infoPair
                );
                BackupShippingTask task = new BackupShippingTask(
                    accCtx,
                    errorReporter,
                    backupApiCallHandler.get(),
                    backupL2LSrcApiCallHandler.get(),
                    config,
                    this,
                    rscDfn.getName().displayValue,
                    prefNode,
                    incremental,
                    lastStartTime
                );
                config.task = task;
                synchronized (syncObj)
                {
                    rscDfnLookupMap.computeIfAbsent(rscDfn, ignored -> new HashSet<>()).add(config);
                    scheduleLookupMap.computeIfAbsent(schedule, ignored -> new HashSet<>()).add(config);
                    remoteLookupMap.computeIfAbsent(remote, ignored -> new HashSet<>()).add(config);
                }
                if (timeout == null)
                {
                    activeShippings.add(config);
                    taskScheduleService.addTask(task);
                }
                else
                {
                    taskScheduleService.rescheduleAt(task, timeout);
                }
            }
        }
        // maybe add else if (running) ...
    }

    public ScheduledShippingConfig getConfig(ResourceDefinition rscDfn, Remote remote, Schedule schedule)
    {
        ScheduledShippingConfig ret = null;
        for (ScheduledShippingConfig conf : rscDfnLookupMap.get(rscDfn))
        {
            if (conf.schedule.equals(schedule) && conf.remote.equals(remote))
            {
                ret = conf;
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
                ZonedDateTime now = ZonedDateTime.now();
                for (ScheduledShippingConfig config : configSet)
                {
                    Pair<Long, Boolean> infoPair = getTimeoutAndType(
                        scheduleRef, accCtx, now, config.task.getPreviousTaskStartTime(), true, false, config.lastInc
                    );
                    config.task.setIncremental(infoPair.objB);
                    taskScheduleService.rescheduleAt(config.task, infoPair.objA);
                }
            }
        }
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
            if (lastFullExecFromNow.isEqual(lastFullExecFromLastStart) || (!lastBackupSucceeded && forceSkip))
            {
                if (nextIncrFromNow.isEqual(nextIncrExecFromLastStart) || (!lastBackupSucceeded && forceSkip))
                {
                    // we are on schedule
                    Pair<Long, Boolean> result = earlier(nextFullFromNow, nextIncrFromNow, nowMillis);
                    timeout = result.objA;
                    incr = result.objB;
                }
                else
                {
                    // we missed an incremental. start asap
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
            if (!lastBackupSucceeded && !forceSkip)
            {
                timeout = 60000L; // TODO: make retry-timeout configurable
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
            removeFromAll(rscDfnLookupMap.get(rscDfnRef));
        }
    }

    public void removeTasks(Schedule scheduleRef, AccessContext accCtx)
        throws InvalidKeyException, AccessDeniedException, DatabaseException
    {
        synchronized (syncObj)
        {
            removeFromAll(scheduleLookupMap.get(scheduleRef));
            removeAllRelatedProps(null, scheduleRef.getName().displayValue, accCtx);
        }
    }

    public void removeTasks(Remote remoteRef, AccessContext accCtx)
        throws InvalidKeyException, AccessDeniedException, DatabaseException
    {
        synchronized (syncObj)
        {
            removeFromAll(remoteLookupMap.get(remoteRef));
            removeAllRelatedProps(remoteRef.getName().displayValue, null, accCtx);
        }
    }

    private void removeFromAll(Set<ScheduledShippingConfig> confsToRemove)
    {
        if (confsToRemove != null)
        {
            for (ScheduledShippingConfig conf : confsToRemove)
            {
                removeSingleTask(conf);
            }
        }
    }

    void removeSingleTask(ScheduledShippingConfig conf)
    {
        if (conf != null)
        {
            synchronized (syncObj)
            {
                activeShippings.remove(conf);
                removeFromSingleMap(remoteLookupMap, conf.remote, conf);
                removeFromSingleMap(scheduleLookupMap, conf.schedule, conf);
                removeFromSingleMap(rscDfnLookupMap, conf.rscDfn, conf);
            }
        }
    }

    public void removeSingleTask(Schedule schedule, Remote remote, ResourceDefinition rscDfn)
    {
        removeSingleTask(new ScheduledShippingConfig(schedule, remote, rscDfn, false, new Pair<>()));
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
            for (String propKey : namespaceProps.get().map().keySet())
            {
                // key for activating scheduled shipping is namespace/{remoteName}/{scheduleName}/Enabled
                String[] splitKey = propKey.split(Props.PATH_SEPARATOR);
                if (
                    remoteName != null && splitKey[1].equals(remoteName) ||
                        scheduleName != null && splitKey[2].equals(scheduleName)
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
        set.remove(conf);
        if (set.isEmpty())
        {
            lookupMap.remove(key);
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
        public final Remote remote;
        public final ResourceDefinition rscDfn;
        public final boolean lastInc;
        public final Pair<Long, Boolean> timeoutAndType;

        private BackupShippingTask task;

        ScheduledShippingConfig(
            Schedule scheduleRef,
            Remote remoteRef,
            ResourceDefinition rscDfnRef,
            boolean lastIncRef,
            Pair<Long, Boolean> timeoutAndTypeRef
        )
        {
            schedule = scheduleRef;
            remote = remoteRef;
            rscDfn = rscDfnRef;
            lastInc = lastIncRef;
            timeoutAndType = timeoutAndTypeRef;
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
    }
}
