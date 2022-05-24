package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.objects.Schedule.InitMaps;
import com.linbit.linstor.core.objects.Schedule.OnFailure;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.ScheduleCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Schedules.DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Schedules.FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Schedules.FULL_CRON;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Schedules.INC_CRON;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Schedules.KEEP_LOCAL;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Schedules.KEEP_REMOTE;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Schedules.NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Schedules.ON_FAILURE;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Schedules.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.function.Function;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;

@Singleton
public class ScheduleDbDriver extends AbsDatabaseDriver<Schedule, Schedule.InitMaps, Void>
    implements ScheduleCtrlDatabaseDriver
{
    protected final PropsContainerFactory propsContainerFactory;
    protected final TransactionObjectFactory transObjFactory;
    protected final Provider<? extends TransactionMgr> transMgrProvider;

    protected final SingleColumnDatabaseDriver<Schedule, Cron> fullCronDriver;
    protected final SingleColumnDatabaseDriver<Schedule, Cron> incCronDriver;
    protected final SingleColumnDatabaseDriver<Schedule, Integer> keepLocalDriver;
    protected final SingleColumnDatabaseDriver<Schedule, Integer> keepRemoteDriver;
    protected final SingleColumnDatabaseDriver<Schedule, OnFailure> onFailureDriver;
    protected final StateFlagsPersistence<Schedule> flagsDriver;
    protected final AccessContext dbCtx;

    @Inject
    public ScheduleDbDriver(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext dbCtxRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.SCHEDULES, dbEngine, objProtDriverRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, schedule -> schedule.getUuid().toString());
        setColumnSetter(NAME, schedule -> schedule.getName().value);
        setColumnSetter(DSP_NAME, schedule -> schedule.getName().displayValue);
        setColumnSetter(FLAGS, schedule -> schedule.getFlags().getFlagsBits(dbCtx));
        setColumnSetter(FULL_CRON, schedule -> schedule.getFullCron(dbCtx).asString());
        setColumnSetter(
            INC_CRON,
            schedule -> schedule.getIncCron(dbCtx) == null ? null : schedule.getIncCron(dbCtx).asString()
        );
        setColumnSetter(KEEP_LOCAL, schedule -> schedule.getKeepLocal(dbCtx));
        setColumnSetter(KEEP_REMOTE, schedule -> schedule.getKeepRemote(dbCtx));
        setColumnSetter(ON_FAILURE, schedule -> schedule.getOnFailure(dbCtx).value);

        fullCronDriver = generateSingleColumnDriver(
            FULL_CRON, schedule -> schedule.getFullCron(dbCtx).asString(), Function.identity()
        );
        incCronDriver = generateSingleColumnDriver(
            INC_CRON,
            schedule -> schedule.getIncCron(dbCtx) == null ? null : schedule.getIncCron(dbCtx).asString(),
            Function.identity()
        );
        keepLocalDriver = generateSingleColumnDriver(
            KEEP_LOCAL, schedule -> "" + schedule.getKeepLocal(dbCtx), Function.identity()
        );
        keepRemoteDriver = generateSingleColumnDriver(
            KEEP_REMOTE, schedule -> "" + schedule.getKeepRemote(dbCtx), Function.identity()
        );
        onFailureDriver = generateSingleColumnDriver(
            ON_FAILURE, schedule -> schedule.getOnFailure(dbCtx).name(), Schedule.OnFailure::getValue
        );

        flagsDriver = generateFlagDriver(FLAGS, Schedule.Flags.class);

    }

    @Override
    public SingleColumnDatabaseDriver<Schedule, Cron> getFullCronDriver()
    {
        return fullCronDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Schedule, Cron> getIncCronDriver()
    {
        return incCronDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Schedule, Integer> getKeepLocalDriver()
    {
        return keepLocalDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Schedule, Integer> getKeepRemoteDriver()
    {
        return keepRemoteDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Schedule, OnFailure> getOnFailureDriver()
    {
        return onFailureDriver;
    }

    @Override
    public StateFlagsPersistence<Schedule> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    protected Pair<Schedule, InitMaps> load(RawParameters raw, Void ignored)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final ScheduleName scheduleName = raw.build(DSP_NAME, ScheduleName::new);
        final long initFlags;
        final long onFailureLong;
        switch (getDbType())
        {
            case ETCD:
                initFlags = Long.parseLong(raw.get(FLAGS));
                onFailureLong = Long.parseLong(raw.get(ON_FAILURE));
                break;
            case SQL: // fall-through
            case K8S_CRD:
                initFlags = raw.get(FLAGS);
                onFailureLong = raw.get(ON_FAILURE);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }
        CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
        return new Pair<Schedule, InitMaps>(
            new Schedule(
                getObjectProtection(ObjectProtection.buildPath(scheduleName)),
                raw.build(UUID, java.util.UUID::fromString),
                this,
                scheduleName,
                initFlags,
                raw.build(FULL_CRON, parser::parse),
                raw.build(INC_CRON, parser::parse),
                raw.get(KEEP_LOCAL),
                raw.get(KEEP_REMOTE),
                OnFailure.getByValueOrNull(onFailureLong),
                transObjFactory,
                transMgrProvider
            ),
            new InitMapsImpl()
        );
    }

    @Override
    protected String getId(Schedule dataRef) throws AccessDeniedException
    {
        return "Schedule(" + dataRef.getName().value + ")";
    }

    private class InitMapsImpl implements Schedule.InitMaps
    {
        private InitMapsImpl()
        {
        }
    }
}
