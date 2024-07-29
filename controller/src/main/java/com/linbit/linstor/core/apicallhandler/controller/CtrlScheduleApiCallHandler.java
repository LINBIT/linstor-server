package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.PriorityProps.MultiResult;
import com.linbit.linstor.PriorityProps.ValueWithDescription;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.SchedulePojo;
import com.linbit.linstor.api.pojo.backups.ScheduleDetailsPojo;
import com.linbit.linstor.api.pojo.backups.ScheduledRscsPojo;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Schedule;
import com.linbit.linstor.core.objects.Schedule.Flags;
import com.linbit.linstor.core.objects.Schedule.OnFailure;
import com.linbit.linstor.core.objects.ScheduleControllerFactory;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.ScheduleRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.ScheduleBackupService;
import com.linbit.linstor.tasks.ScheduleBackupService.ScheduledShippingConfig;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlScheduleApiCallHandler
{
    private static final String REASON_NONE_SET = "none set";
    private static final String REASON_DISABLED = "disabled";
    private static final String REASON_UNDEPLOYED = "undeployed";

    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final ResponseConverter responseConverter;
    private final ScheduleControllerFactory scheduleFactory;
    private final ScheduleRepository scheduleRepository;
    private final ResourceDefinitionRepository rscDfnRepo;
    private final SystemConfRepository systemConfRepository;
    private final ScheduleBackupService scheduleService;

    @Inject
    public CtrlScheduleApiCallHandler(
        @SystemContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        ResponseConverter responseConverterRef,
        ScheduleControllerFactory scheduleFactoryRef,
        ScheduleRepository scheduleRepositoryRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        SystemConfRepository systemConfRepositoryRef,
        ScheduleBackupService scheduleServiceRef
    )
    {
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        scopeRunner = scopeRunnerRef;
        responseConverter = responseConverterRef;
        scheduleFactory = scheduleFactoryRef;
        scheduleRepository = scheduleRepositoryRef;
        rscDfnRepo = rscDfnRepoRef;
        systemConfRepository = systemConfRepositoryRef;
        scheduleService = scheduleServiceRef;
    }

    public List<SchedulePojo> listSchedule()
    {
        ArrayList<SchedulePojo> ret = new ArrayList<>();
        try (LockGuard lg = lockGuardFactory.build(LockType.READ, LockObj.SCHEDULE_MAP))
        {
            AccessContext pAccCtx = peerAccCtx.get();
            for (Entry<ScheduleName, Schedule> entry : scheduleRepository.getMapForView(pAccCtx).entrySet())
            {
                if (entry.getValue() instanceof Schedule)
                {
                    ret.add(entry.getValue().getApiData(pAccCtx, null, null));
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            // ignore, we will return an empty list
        }
        return ret;
    }

    public Flux<ApiCallRc> createSchedule(
        String scheduleName,
        String fullCronRef,
        @Nullable String incCronRef,
        @Nullable Integer keepLocalRef,
        @Nullable Integer keepRemoteRef,
        @Nullable String onFailureRef,
        @Nullable Integer maxRetriesRef
    )
    {
        ResponseContext context = makeScheduleContext(
            ApiOperation.makeCreateOperation(),
            scheduleName
        );

        return scopeRunner.fluxInTransactionalScope(
            "Create schedule",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.SCHEDULE_MAP),
            () -> createScheduleInTransaction(
                scheduleName, fullCronRef, incCronRef, keepLocalRef, keepRemoteRef, onFailureRef, maxRetriesRef
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createScheduleInTransaction(
        String scheduleNameStr,
        String fullCronRef,
        @Nullable String incCronRef,
        @Nullable Integer keepLocalRef,
        @Nullable Integer keepRemoteRef,
        @Nullable String onFailureRef,
        @Nullable Integer maxRetriesRef
    )
    {
        ScheduleName scheduleName = LinstorParsingUtils.asScheduleName(scheduleNameStr);
        if (ctrlApiDataLoader.loadSchedule(scheduleName, false) != null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SCHEDULE,
                    "A schedule with the name '" + scheduleNameStr
                       + "' already exists. Please use a different name or try to modify the schedule instead."
                )
            );
        }
        Schedule schedule = null;

        OnFailure failureType = onFailureRef == null ? OnFailure.SKIP : OnFailure.valueOfIgnoreCase(onFailureRef);
        String scheduleDescription = "";
        try
        {
            schedule = scheduleFactory
                .create(
                    peerAccCtx.get(), scheduleName, fullCronRef, incCronRef, keepLocalRef, keepRemoteRef, failureType,
                    maxRetriesRef
                );
            scheduleRepository.put(apiCtx, schedule);
            scheduleDescription = getDetailedScheduleDescription(schedule);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "create " + getScheduleDescription(scheduleNameStr),
                ApiConsts.FAIL_ACC_DENIED_SCHEDULE
            );
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            throw new ImplementationError(exc);
        }

        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }

        ctrlTransactionHelper.commit();
        ApiCallRcImpl responses = new ApiCallRcImpl();
        responses.addEntry(
            ApiCallRcImpl
                .entryBuilder(ApiConsts.CREATED, scheduleDescription + " created sucessfully.")
                .setDetails(scheduleDescription + " UUID is: " + schedule.getUuid().toString())
                .build()
        );
        return Flux.<ApiCallRc> just(responses);
    }

    public Flux<ApiCallRc> changeSchedule(
        String scheduleNameStr,
        String fullCronRef,
        String incCronRef,
        Integer keepLocalRef,
        Integer keepRemoteRef,
        String onFailureRef,
        Integer maxRetriesRef
    )
    {
        ResponseContext context = makeScheduleContext(
            ApiOperation.makeModifyOperation(),
            scheduleNameStr
        );

        return scopeRunner.fluxInTransactionalScope(
            "Modify schedule",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.SCHEDULE_MAP),
            () -> changeScheduleInTransaction(
                scheduleNameStr, fullCronRef, incCronRef, keepLocalRef, keepRemoteRef, onFailureRef, maxRetriesRef
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> changeScheduleInTransaction(
        String scheduleNameStr,
        String fullCronRef,
        String incCronRef,
        Integer keepLocalRef,
        Integer keepRemoteRef,
        String onFailureRef,
        Integer maxRetriesRef
    )
    {
        ScheduleName scheduleName = LinstorParsingUtils.asScheduleName(scheduleNameStr);
        Schedule schedule = ctrlApiDataLoader.loadSchedule(scheduleName, true);
        boolean modifyTask = false;
        String scheduleDescription = getScheduleDescription(scheduleName.displayValue);
        try
        {
            if (fullCronRef != null && !fullCronRef.isEmpty())
            {
                try
                {
                    schedule.setFullCron(peerAccCtx.get(), fullCronRef);
                }
                catch (IllegalArgumentException exc)
                {
                    throw new ApiException("Error parsing full-cron: " + exc.getMessage(), exc);
                }
                modifyTask = true;
            }
            if (incCronRef != null)
            {
                if (incCronRef.isEmpty())
                {
                    schedule.setIncCron(peerAccCtx.get(), null);
                }
                else
                {
                    try
                    {
                        schedule.setIncCron(peerAccCtx.get(), incCronRef);
                    }
                    catch (IllegalArgumentException exc)
                    {
                        throw new ApiException("Error parsing inc-cron: " + exc.getMessage(), exc);
                    }
                }
                modifyTask = true;
            }
            if (keepLocalRef != null)
            {
                if (keepLocalRef < 0)
                {
                    schedule.setKeepLocal(peerAccCtx.get(), null);
                }
                else
                {
                    schedule.setKeepLocal(peerAccCtx.get(), keepLocalRef);
                }
            }
            if (keepRemoteRef != null)
            {
                if (keepRemoteRef < 0)
                {
                    schedule.setKeepRemote(peerAccCtx.get(), null);
                }
                else
                {
                    schedule.setKeepRemote(peerAccCtx.get(), keepRemoteRef);
                }
            }
            if (onFailureRef != null && !onFailureRef.isEmpty())
            {
                schedule.setOnFailure(peerAccCtx.get(), onFailureRef);
            }
            if (maxRetriesRef != null)
            {
                if (maxRetriesRef < 0)
                {
                    schedule.setMaxRetries(peerAccCtx.get(), null);
                }
                else
                {
                    schedule.setMaxRetries(peerAccCtx.get(), maxRetriesRef);
                }
            }
            ctrlTransactionHelper.commit();
            if (modifyTask)
            {
                scheduleService.modifyTasks(schedule, peerAccCtx.get());
                scheduleDescription = getDetailedScheduleDescription(schedule);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "modify " + getScheduleDescription(scheduleNameStr),
                ApiConsts.FAIL_ACC_DENIED_SCHEDULE
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }


        ApiCallRcImpl responses = new ApiCallRcImpl();
        responses.addEntry(
            ApiCallRcImpl
                .entryBuilder(ApiConsts.MODIFIED, scheduleDescription + " modified sucessfully.")
                .setDetails(scheduleDescription + " UUID is: " + schedule.getUuid().toString())
                .build()
        );
        return Flux.<ApiCallRc> just(responses);
    }

    public Flux<ApiCallRc> delete(String scheduleNameStrRef)
    {
        ResponseContext context = makeScheduleContext(
            ApiOperation.makeDeleteOperation(),
            scheduleNameStrRef
        );
        // take extra locks because schedule-related props might need to be deleted as well
        return scopeRunner.fluxInTransactionalScope(
            "Delete schedule",
            lockGuardFactory.buildDeferred(
                LockType.WRITE, LockObj.SCHEDULE_MAP, LockObj.RSC_DFN_MAP, LockObj.RSC_GRP_MAP, LockObj.CTRL_CONFIG
            ),
            () -> deleteInTransaction(scheduleNameStrRef)
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteInTransaction(String scheduleNameStrRef)
    {
        Flux<ApiCallRc> flux;
        ScheduleName scheduleName = LinstorParsingUtils.asScheduleName(scheduleNameStrRef);
        Schedule schedule = ctrlApiDataLoader.loadSchedule(scheduleName, false);
        String scheduleDescription = getScheduleDescription(scheduleNameStrRef);

        if (schedule == null)
        {
            flux = Flux.<ApiCallRc> just(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.WARN_NOT_FOUND,
                    scheduleDescription + " not found."
                )
            );
        }
        else
        {
            enableFlags(schedule, Schedule.Flags.DELETE);
            try
            {
                scheduleService.removeTasks(schedule, peerAccCtx.get());
            }
            catch (InvalidKeyException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (AccessDeniedException exc)
            {
                throw new ApiAccessDeniedException(
                    exc,
                    "while trying to remove props",
                    ApiConsts.FAIL_ACC_DENIED_SCHEDULE
                );
            }
            catch (DatabaseException exc)
            {
                throw new ApiDatabaseException(exc);
            }
            ctrlTransactionHelper.commit();
            ApiCallRcImpl responses = new ApiCallRcImpl();
            responses.addEntry(
                ApiCallRcImpl
                    .entryBuilder(ApiConsts.DELETED, scheduleDescription + " marked for deletion.")
                    .setDetails(scheduleDescription + " UUID is: " + schedule.getUuid().toString())
                    .build()
            );
            flux = Flux.<ApiCallRc> just(responses)
                .concatWith(deleteImpl(schedule));
        }
        return flux;
    }

    public List<ScheduledRscsPojo> listScheduledRscs(
        @Nullable String rscName,
        @Nullable String remoteName,
        @Nullable String scheduleName,
        boolean activeOnly
    )
    {
        List<ScheduledRscsPojo> ret = new ArrayList<>();
        try (
            LockGuard lg = lockGuardFactory
                .build(LockType.READ, LockObj.SCHEDULE_MAP, LockObj.RSC_DFN_MAP, LockObj.REMOTE_MAP)
        )
        {
            ret.addAll(listActiveSchedules(rscName, remoteName, scheduleName));
            if (!activeOnly)
            {
                ret.addAll(listUnscheduledRsc(rscName));
            }
            ret.sort((a, b) ->
            {
                int cmp = a.rsc_name.compareTo(b.rsc_name);
                if (cmp == 0)
                {
                    cmp = StringUtils.compareToNullable(a.remote, b.remote);
                    if (cmp == 0)
                    {
                        cmp = StringUtils.compareToNullable(a.schedule, b.schedule);
                    }
                }
                return cmp;
            });
        }
        catch (AccessDeniedException exc)
        {
            // ignore, we will return an empty list
        }
        return ret;
    }

    private List<ScheduledRscsPojo> listUnscheduledRsc(@Nullable String rscName)
        throws AccessDeniedException
    {
        // Map<rscName, Pair<reason, List<Pair<remoteName, scheduleName>>>>
        List<ScheduledRscsPojo> ret = new ArrayList<>();
        ResourceDefinitionMap map = rscDfnRepo.getMapForView(peerAccCtx.get());
        for (ResourceDefinition rscDfn : map.values())
        {
            if (rscName == null || rscName.isEmpty() || rscDfn.getName().value.equalsIgnoreCase(rscName))
            {
                PriorityProps prioProps = new PriorityProps(
                    rscDfn.getProps(peerAccCtx.get()),
                    rscDfn.getResourceGroup().getProps(peerAccCtx.get()),
                    systemConfRepository.getCtrlConfForView(peerAccCtx.get())
                );
                Map<String, String> props = prioProps.renderRelativeMap(InternalApiConsts.NAMESPC_SCHEDULE);
                if (props.size() == 0)
                {
                    ret.add(
                        new ScheduledRscsPojo(
                            rscDfn.getName().displayValue,
                            null,
                            null,
                            REASON_NONE_SET,
                            -1,
                            false,
                            -1,
                            false,
                            -1,
                            -1
                        )
                    );
                }
                else
                {
                    boolean confSet = false;
                    List<Pair<String, String>> confs = new ArrayList<>();
                    for (Entry<String, String> prop : props.entrySet())
                    {
                        // key is {remoteName}/{scheduleName}
                        String[] parts = prop.getKey().split(ReadOnlyProps.PATH_SEPARATOR);
                        if (parts.length == 3 && parts[2].equals(InternalApiConsts.KEY_TRIPLE_ENABLED))
                        {
                            if (prop.getValue().equals(ApiConsts.VAL_FALSE))
                            {
                                confs.add(new Pair<>(parts[0], parts[1]));
                            }
                            else
                            {
                                confSet = true;
                                break;
                            }
                        }
                    }
                    if (!confSet)
                    {
                        if (confs.size() == 0)
                        {
                            ret.add(
                                new ScheduledRscsPojo(
                                    rscDfn.getName().displayValue,
                                    null,
                                    null,
                                    REASON_NONE_SET,
                                    -1,
                                    false,
                                    -1,
                                    false,
                                    -1,
                                    -1
                                )
                            );
                        }
                        else
                        {
                            for (Pair<String, String> conf : confs)
                            {
                                ret.add(
                                    new ScheduledRscsPojo(
                                        rscDfn.getName().displayValue,
                                        conf.objA,
                                        conf.objB,
                                        REASON_DISABLED,
                                        -1,
                                        false,
                                        -1,
                                        false,
                                        -1,
                                        -1
                                    )
                                );
                            }
                        }
                    }
                    else
                    {
                        if (rscDfn.getResourceCount() == 0)
                        {
                            // this will not be listed under activeSchedules, so add it here
                            ret.add(
                                new ScheduledRscsPojo(
                                    rscDfn.getName().displayValue,
                                    null,
                                    null,
                                    REASON_UNDEPLOYED,
                                    -1,
                                    false,
                                    -1,
                                    false,
                                    -1,
                                    -1
                                )
                            );
                        }
                    }
                }
            }
        }
        return ret;
    }

    private List<ScheduledRscsPojo> listActiveSchedules(
        @Nullable String rscName,
        @Nullable String remoteName,
        @Nullable String scheduleName
    )
        throws AccessDeniedException
    {
        List<ScheduledRscsPojo> ret = new ArrayList<>();
        Set<ScheduledShippingConfig> activeShippings = scheduleService
            .getAllFilteredActiveShippings(rscName, remoteName, scheduleName);
        for (ScheduledShippingConfig conf : activeShippings)
        {
            String lastSnapTime = conf.rscDfn.getProps(peerAccCtx.get()).getProp(
                conf.remote.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR + conf.schedule.getName().displayValue
                    + ReadOnlyProps.PATH_SEPARATOR + InternalApiConsts.KEY_LAST_BACKUP_TIME,
                InternalApiConsts.NAMESPC_SCHEDULE
            );
            long lastSnapTimeLong;
            ZonedDateTime lastSnap;
            if (lastSnapTime != null)
            {
                lastSnapTimeLong = Long.parseLong(lastSnapTime);
                lastSnap = ZonedDateTime.ofInstant(Instant.ofEpochMilli(lastSnapTimeLong), ZoneId.systemDefault());
            }
            else
            {
                lastSnapTimeLong = -1;
                lastSnap = ZonedDateTime.now();
            }
            String lastSnapInc = conf.rscDfn.getProps(peerAccCtx.get()).getProp(
                conf.remote.getName().displayValue + ReadOnlyProps.PATH_SEPARATOR + conf.schedule.getName().displayValue
                    + ReadOnlyProps.PATH_SEPARATOR + InternalApiConsts.KEY_LAST_BACKUP_INC,
                InternalApiConsts.NAMESPC_SCHEDULE
            );
            if (lastSnapTimeLong >= 0 && lastSnapInc == null)
            {
                throw new ImplementationError("KEY_LAST_BACKUP_INC needs to be set if KEY_LAST_BACKUP_TIME is set");
            }
            long nextPlannedFull = -1;
            long nextPlannedInc = -1;
            Optional<ZonedDateTime> nextExecFull = ExecutionTime.forCron(conf.schedule.getFullCron(peerAccCtx.get()))
                .nextExecution(lastSnap);
            if (nextExecFull.isPresent())
            {
                nextPlannedFull = nextExecFull.get().toEpochSecond() * 1000;
            }
            if (conf.schedule.getIncCron(peerAccCtx.get()) != null)
            {
                Optional<ZonedDateTime> nextExecInc = ExecutionTime.forCron(conf.schedule.getIncCron(peerAccCtx.get()))
                    .nextExecution(lastSnap);
                if (nextExecInc.isPresent())
                {
                    nextPlannedInc = nextExecInc.get().toEpochSecond() * 1000;
                }
            }
            ScheduledShippingConfig currentConf = scheduleService.getConfig(conf.rscDfn, conf.remote, conf.schedule);
            long nextExecTime = -1;
            boolean nextExecInc = false;
            if (currentConf != null)
            {
                Pair<Long, Boolean> pair = currentConf.timeoutAndType;
                Long pairCalculatedFrom = currentConf.timeoutAndTypeCalculatedFrom;
                if (pair.objA != null)
                {
                    if (pairCalculatedFrom != null)
                    {
                        nextExecTime = pairCalculatedFrom + pair.objA;
                    }
                    else
                    {
                        throw new ImplementationError(
                            "timeoutAndType of conf " + currentConf +
                                " was set without also setting timeoutAndTypeCalculatedFrom"
                        );
                    }
                }
                else
                {
                    nextExecTime = -1;
                }
                nextExecInc = pair.objB != null && pair.objB;

            }
            ScheduledRscsPojo pojo = new ScheduledRscsPojo(
                conf.rscDfn.getName().displayValue,
                conf.remote.getName().displayValue,
                conf.schedule.getName().displayValue,
                null,
                lastSnapTimeLong,
                ApiConsts.VAL_TRUE.equals(lastSnapInc),
                nextExecTime,
                nextExecInc,
                nextPlannedFull,
                nextPlannedInc
            );
            ret.add(pojo);
        }
        return ret;
    }

    public List<ScheduleDetailsPojo> listScheduleDetails(String rscNameRef)
    {
        List<ScheduleDetailsPojo> ret = new ArrayList<>();
        try (
            LockGuard lg = lockGuardFactory.build(
                LockType.READ, LockObj.RSC_DFN_MAP, LockObj.RSC_GRP_MAP, LockObj.CTRL_CONFIG, LockObj.REMOTE_MAP,
                LockObj.SCHEDULE_MAP
            )
        )
        {
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
            PriorityProps prioProps = new PriorityProps();
            final String rscDfnStr = "rscDfn";
            final String rscGrpStr = "rscGrp";
            final String ctrlStr = "ctrl";
            prioProps.addProps(rscDfn.getProps(peerAccCtx.get()), rscDfnStr);
            prioProps.addProps(rscDfn.getResourceGroup().getProps(peerAccCtx.get()), rscGrpStr);
            prioProps.addProps(systemConfRepository.getCtrlConfForView(peerAccCtx.get()), ctrlStr);
            Map<String, MultiResult> propsMap = prioProps
                .renderConflictingMap(InternalApiConsts.NAMESPC_SCHEDULE, false);
            for (Entry<String, MultiResult> prop : propsMap.entrySet())
            {
                String[] splitKey = prop.getKey().split(ReadOnlyProps.PATH_SEPARATOR);
                if (splitKey.length == 3 && splitKey[2].equals(InternalApiConsts.KEY_TRIPLE_ENABLED))
                {
                    String remote = splitKey[0];
                    String schedule = splitKey[1];
                    Boolean ctrl = null;
                    Boolean grp = null;
                    Boolean dfn = null;
                    // first needs to be checked, but is not part of conflictingList
                    List<ValueWithDescription> allValsToCheck = new ArrayList<>(prop.getValue().conflictingList);
                    allValsToCheck.add(prop.getValue().first);
                    for (ValueWithDescription entry : allValsToCheck)
                    {
                        switch (entry.propsDescription)
                        {
                            case rscDfnStr:
                                dfn = getBoolFromProp(entry.value);
                                break;
                            case rscGrpStr:
                                grp = getBoolFromProp(entry.value);
                                break;
                            case ctrlStr:
                                ctrl = getBoolFromProp(entry.value);
                                break;
                            default:
                                throw new ImplementationError(
                                    "unknown case reached: propsDescription was " + entry.propsDescription
                                );
                        }
                    }
                    ret.add(new ScheduleDetailsPojo(remote, schedule, ctrl, grp, dfn));
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            // ignore, we will return an empty list
        }
        return ret;
    }

    private @Nullable Boolean getBoolFromProp(String str)
    {
        Boolean ret = null;
        if (str.equals(ApiConsts.VAL_TRUE))
        {
            ret = true;
        }
        else if (str.equals(ApiConsts.VAL_FALSE))
        {
            ret = false;
        }
        return ret;
    }

    private Flux<ApiCallRc> deleteImpl(Schedule scheduleRef)
    {
        ResponseContext context = makeScheduleContext(
            ApiOperation.makeModifyOperation(),
            scheduleRef.getName().displayValue
        );

        return scopeRunner.fluxInTransactionalScope(
            "Delete schedule impl",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.SCHEDULE_MAP),
            () -> deleteImplInTransaction(scheduleRef)
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteImplInTransaction(Schedule scheduleRef)
    {
        ScheduleName scheduleName = scheduleRef.getName();
        String scheduleDescription = getScheduleDescription(scheduleName.displayValue);
        UUID uuid = scheduleRef.getUuid();

        try
        {
            scheduleRef.delete(peerAccCtx.get());
            scheduleRepository.remove(apiCtx, scheduleName);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "delete " + scheduleDescription,
                ApiConsts.FAIL_ACC_DENIED_SCHEDULE
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }

        ctrlTransactionHelper.commit();

        ApiCallRcImpl.ApiCallRcEntry response = ApiCallRcImpl
            .entryBuilder(ApiConsts.DELETED, scheduleDescription + " deleted.")
            .setDetails(scheduleDescription + " UUID was: " + uuid.toString())
            .build();
        return Flux.just(new ApiCallRcImpl(response));
    }

    private void enableFlags(Schedule scheduleRef, Flags... flags)
    {
        try
        {
            scheduleRef.getFlags().enableFlags(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "delete " + getScheduleDescription(scheduleRef.getName().displayValue),
                ApiConsts.FAIL_ACC_DENIED_SCHEDULE
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    public static ResponseContext makeScheduleContext(
        ApiOperation operation,
        String nameRef
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_REMOTE, nameRef);

        return new ResponseContext(
            operation,
            getScheduleDescription(nameRef),
            getScheduleDescriptionInline(nameRef),
            ApiConsts.MASK_SCHEDULE,
            objRefs
        );
    }

    private String getDetailedScheduleDescription(Schedule scheduleRef) throws AccessDeniedException
    {
        CronDescriptor descriptor = CronDescriptor.instance(Locale.US);
        String fullDescription = descriptor.describe(scheduleRef.getFullCron(apiCtx));
        String incDescription = "";
        Cron incCron = scheduleRef.getIncCron(apiCtx);
        if (incCron != null)
        {
            incDescription = ", IncCron: " + descriptor.describe(incCron);
        }
        return "Schedule: " + scheduleRef.getName().displayValue + ", FullCron: " + fullDescription + incDescription;
    }

    public static String getScheduleDescription(String pathRef)
    {
        return "Schedule: " + pathRef;
    }

    public static String getScheduleDescriptionInline(String pathRef)
    {
        return "schedule '" + pathRef + "'";
    }
}
