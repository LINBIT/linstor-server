package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.SchedulePojo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.objects.Schedule;
import com.linbit.linstor.core.objects.Schedule.Flags;
import com.linbit.linstor.core.objects.Schedule.OnFailure;
import com.linbit.linstor.core.objects.ScheduleControllerFactory;
import com.linbit.linstor.core.repository.ScheduleRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlScheduleApiCallHandler
{
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final ResponseConverter responseConverter;
    private final ScheduleControllerFactory scheduleFactory;
    private final ScheduleRepository scheduleRepository;

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
        ScheduleRepository scheduleRepositoryRef
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
    }

    public List<SchedulePojo> listSchedule()
    {
        ArrayList<SchedulePojo> ret = new ArrayList<>();
        try
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
        String incCronRef,
        int keepLocalRef,
        int keepRemoteRef,
        String onFailureRef
    )
    {
        ResponseContext context = makeScheduleContext(
            ApiOperation.makeModifyOperation(),
            scheduleName
        );

        return scopeRunner.fluxInTransactionalScope(
            "Create schedule",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.SCHEDULE_MAP),
            () -> createScheduleInTransaction(
                scheduleName, fullCronRef, incCronRef, keepLocalRef, keepRemoteRef, onFailureRef
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createScheduleInTransaction(
        String scheduleNameStr,
        String fullCronRef,
        String incCronRef,
        int keepLocalRef,
        int keepRemoteRef,
        String onFailureRef
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

        OnFailure failureType = OnFailure.valueOfIgnoreCase(onFailureRef);

        try
        {
            schedule = scheduleFactory
                .create(peerAccCtx.get(), scheduleName, fullCronRef, incCronRef, keepLocalRef, keepRemoteRef, failureType);
            scheduleRepository.put(apiCtx, schedule);
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
        String scheduleDescription = getScheduleDescription(scheduleName.displayValue);
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
        String onFailureRef
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
                scheduleNameStr, fullCronRef, incCronRef, keepLocalRef, keepRemoteRef, onFailureRef
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> changeScheduleInTransaction(
        String scheduleNameStr,
        String fullCronRef,
        String incCronRef,
        Integer keepLocalRef,
        Integer keepRemoteRef,
        String onFailureRef
    )
    {
        ScheduleName scheduleName = LinstorParsingUtils.asScheduleName(scheduleNameStr);
        Schedule schedule = ctrlApiDataLoader.loadSchedule(scheduleName, true);
        try
        {
            if (fullCronRef != null && !fullCronRef.isEmpty())
            {
                schedule.setFullCron(peerAccCtx.get(), fullCronRef);
            }
            if (incCronRef != null && !incCronRef.isEmpty())
            {
                schedule.setIncCron(peerAccCtx.get(), incCronRef);
            }
            if (keepLocalRef != null)
            {
                schedule.setKeepLocal(peerAccCtx.get(), keepLocalRef);
            }
            if (keepRemoteRef != null)
            {
                schedule.setKeepRemote(peerAccCtx.get(), keepRemoteRef);
            }
            if (onFailureRef != null && !onFailureRef.isEmpty())
            {
                schedule.setOnFailure(peerAccCtx.get(), onFailureRef);
            }
            ctrlTransactionHelper.commit();
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

        String scheduleDescription = getScheduleDescription(scheduleName.displayValue);
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

    public static String getScheduleDescription(String pathRef)
    {
        return "Schedule: " + pathRef;
    }

    public static String getScheduleDescriptionInline(String pathRef)
    {
        return "schedule '" + pathRef + "'";
    }
}
