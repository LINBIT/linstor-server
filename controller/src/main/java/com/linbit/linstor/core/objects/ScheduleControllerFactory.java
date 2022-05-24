package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
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

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.UUID;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
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
        String incCron,
        int keepLocal,
        int keepRemote,
        OnFailure onFailure
    )
        throws AccessDeniedException, LinStorDataAlreadyExistsException, DatabaseException
    {
        if (scheduleRepo.get(accCtxRef, nameRef) != null)
        {
            throw new LinStorDataAlreadyExistsException("This schedule name is already registered");
        }
        CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
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
            parser.parse(fullCron),
            parser.parse(incCron),
            keepLocal,
            keepRemote,
            onFailure,
            transObjFactory,
            transMgrProvider
        );

        dbDriver.create(schedule);

        return schedule;
    }
}
