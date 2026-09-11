package com.linbit.linstor.core.objects;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.objects.Schedule.InitMaps;
import com.linbit.linstor.core.objects.Schedule.OnFailure;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.ScheduleCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
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
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Schedules.MAX_RETRIES;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Schedules.NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Schedules.ON_FAILURE;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Schedules.UUID;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.function.Function;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;

@Singleton
public final class ScheduleDbDriver extends AbsDatabaseDriver<Schedule, Schedule.InitMaps, Void>
    implements ScheduleCtrlDatabaseDriver
{
    final PropsContainerFactory propsContainerFactory;
    final TransactionObjectFactory transObjFactory;
    final Provider<? extends TransactionMgr> transMgrProvider;

    final SingleColumnDatabaseDriver<Schedule, Cron> fullCronDriver;
    final SingleColumnDatabaseDriver<Schedule, Cron> incCronDriver;
    final SingleColumnDatabaseDriver<Schedule, Integer> keepLocalDriver;
    final SingleColumnDatabaseDriver<Schedule, Integer> keepRemoteDriver;
    final SingleColumnDatabaseDriver<Schedule, OnFailure> onFailureDriver;
    final SingleColumnDatabaseDriver<Schedule, Integer> maxRetriesDriver;
    final StateFlagsPersistence<Schedule> flagsDriver;

    @Inject
    public ScheduleDbDriver(
        ErrorReporter errorReporterRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.SCHEDULES, dbEngine);
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, schedule -> schedule.getUuid().toString());
        setColumnSetter(NAME, schedule -> schedule.getName().value);
        setColumnSetter(DSP_NAME, schedule -> schedule.getName().displayValue);
        setColumnSetter(FLAGS, schedule -> schedule.getFlags().getFlagsBits());
        setColumnSetter(FULL_CRON, schedule -> schedule.getFullCron().asString());
        setColumnSetter(
            INC_CRON,
            schedule -> schedule.getIncCron() == null ? null : schedule.getIncCron().asString()
        );
        setColumnSetter(KEEP_LOCAL, schedule -> schedule.getKeepLocal());
        setColumnSetter(KEEP_REMOTE, schedule -> schedule.getKeepRemote());
        setColumnSetter(ON_FAILURE, schedule -> schedule.getOnFailure().value);
        setColumnSetter(MAX_RETRIES, schedule -> schedule.getMaxRetries());

        fullCronDriver = generateSingleColumnDriver(
            FULL_CRON, schedule -> schedule.getFullCron().asString(), Cron::asString, Cron::asString
        );
        incCronDriver = generateSingleColumnDriver(
            INC_CRON,
            schedule -> schedule.getIncCron() == null ? null : schedule.getIncCron().asString(),
            incCron -> incCron == null ? null : incCron.asString(),
            incCron -> incCron == null ? null : incCron.asString()
        );
        keepLocalDriver = generateSingleColumnDriver(
            KEEP_LOCAL, schedule -> "" + schedule.getKeepLocal(), Function.identity()
        );
        keepRemoteDriver = generateSingleColumnDriver(
            KEEP_REMOTE, schedule -> "" + schedule.getKeepRemote(), Function.identity()
        );
        onFailureDriver = generateSingleColumnDriver(
            ON_FAILURE, schedule -> schedule.getOnFailure().name(), Schedule.OnFailure::getValue
        );
        maxRetriesDriver = generateSingleColumnDriver(
            MAX_RETRIES, schedule -> "" + schedule.getMaxRetries(), Function.identity()
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
    public SingleColumnDatabaseDriver<Schedule, Integer> getMaxRetriesDriver()
    {
        return maxRetriesDriver;
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
        final Integer keepLocal;
        final Integer keepRemote;
        final Integer maxRetries;
        initFlags = raw.get(FLAGS);
        onFailureLong = raw.get(ON_FAILURE);
        keepLocal = raw.get(KEEP_LOCAL);
        keepRemote = raw.get(KEEP_REMOTE);
        maxRetries = raw.get(MAX_RETRIES);
        CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
        return new Pair<>(
            new Schedule(
                raw.build(UUID, java.util.UUID::fromString),
                this,
                scheduleName,
                initFlags,
                raw.build(FULL_CRON, parser::parse),
                raw.build(INC_CRON, parser::parse),
                keepLocal,
                keepRemote,
                OnFailure.getByValueOrNull(onFailureLong),
                maxRetries,
                transObjFactory,
                transMgrProvider
            ),
            new InitMapsImpl()
        );
    }

    @Override
    protected String getId(Schedule dataRef)
    {
        return "Schedule(" + dataRef.getName().value + ")";
    }

    private static class InitMapsImpl implements Schedule.InitMaps
    {
        private InitMapsImpl()
        {
        }
    }
}
