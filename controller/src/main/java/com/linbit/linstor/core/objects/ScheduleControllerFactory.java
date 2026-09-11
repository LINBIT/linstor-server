package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.objects.Schedule.OnFailure;
import com.linbit.linstor.core.repository.ScheduleRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ScheduleDatabaseDriver;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

@Singleton
public class ScheduleControllerFactory
{
    private final ScheduleDatabaseDriver dbDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ScheduleRepository scheduleRepo;

    @Inject
    public ScheduleControllerFactory(
        ScheduleDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ScheduleRepository scheduleRepoRef
    )
    {
        dbDriver = dbDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        scheduleRepo = scheduleRepoRef;
    }

    public Schedule create(
        ScheduleName nameRef,
        String fullCron,
        @Nullable String incCron,
        @Nullable Integer keepLocal,
        @Nullable Integer keepRemote,
        OnFailure onFailure,
        @Nullable Integer maxRetries
    )
        throws LinStorDataAlreadyExistsException, DatabaseException
    {
        if (scheduleRepo.get(nameRef) != null)
        {
            throw new LinStorDataAlreadyExistsException("This schedule name is already registered");
        }
        CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
        Cron parsedFull;
        try
        {
            parsedFull = parser.parse(fullCron);
        }
        catch (IllegalArgumentException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_ERROR | ApiConsts.MASK_SCHEDULE,
                "Error parsing full-cron \"" + fullCron + "\": " + exc.getMessage(),
                true
                ));
        }
        Cron parsedInc;
        try
        {
            parsedInc = incCron == null ? null : parser.parse(incCron);
        }
        catch (IllegalArgumentException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_ERROR | ApiConsts.MASK_SCHEDULE,
                "Error parsing full-cron \"" + incCron + "\": " + exc.getMessage(),
                true
            ));
        }
        ZonedDateTime now = ZonedDateTime.now(ZoneId.systemDefault());
        ApiCallRcImpl errorRc = null;
        if (!ExecutionTime.forCron(parsedFull).nextExecution(now).isPresent())
        {
            errorRc = new ApiCallRcImpl();
            errorRc.addEntry(
                "The cron expression for full backups '" + fullCron + "' has no possible future executions.",
                ApiConsts.MASK_ERROR | ApiConsts.MASK_SCHEDULE
            );
        }
        if (parsedInc != null && !ExecutionTime.forCron(parsedInc).nextExecution(now).isPresent())
        {
            if (errorRc == null)
            {
                errorRc = new ApiCallRcImpl();
            }
            errorRc.addEntry(
                "The cron expression for inc backups '" + incCron + "' has no possible future executions.",
                ApiConsts.MASK_ERROR | ApiConsts.MASK_SCHEDULE
            );
        }
        if (errorRc != null)
        {
            throw new ApiRcException(errorRc);
        }
        Schedule schedule = new Schedule(
            UUID.randomUUID(),
            dbDriver,
            nameRef,
            0,
            parsedFull,
            parsedInc,
            keepLocal,
            keepRemote,
            onFailure,
            maxRetries,
            transObjFactory,
            transMgrProvider
        );

        dbDriver.create(schedule);

        return schedule;
    }
}
