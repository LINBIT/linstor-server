package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.objects.Schedule.OnFailure;
import com.linbit.linstor.core.repository.ScheduleRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ScheduleDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import com.linbit.linstor.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

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
    private final ObjectProtectionFactory objProtFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ScheduleRepository scheduleRepo;

    @Inject
    public ScheduleControllerFactory(
        ScheduleDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objProtFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ScheduleRepository scheduleRepoRef
    )
    {
        dbDriver = dbDriverRef;
        objProtFactory = objProtFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        scheduleRepo = scheduleRepoRef;
    }

    public Schedule create(
        AccessContext accCtxRef,
        ScheduleName nameRef,
        String fullCron,
        @Nullable String incCron,
        @Nullable Integer keepLocal,
        @Nullable Integer keepRemote,
        OnFailure onFailure,
        @Nullable Integer maxRetries
    )
        throws AccessDeniedException, LinStorDataAlreadyExistsException, DatabaseException
    {
        if (scheduleRepo.get(accCtxRef, nameRef) != null)
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
            throw new ApiException("Error parsing full-cron: " + exc.getMessage(), exc);
        }
        Cron parsedInc;
        try
        {
            parsedInc = incCron == null ? null : parser.parse(incCron);
        }
        catch (IllegalArgumentException exc)
        {
            throw new ApiException("Error parsing inc-cron: " + exc.getMessage(), exc);
        }
        ZonedDateTime now = ZonedDateTime.now();
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
            objProtFactory.getInstance(
                accCtxRef,
                ObjectProtection.buildPath(nameRef),
                true
            ),
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
