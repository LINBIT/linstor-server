package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.Schedule;
import com.linbit.linstor.core.objects.Schedule.OnFailure;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import com.cronutils.model.Cron;

public interface ScheduleDatabaseDriver
{
    /**
     * Creates or updates the given Schedule object into the database.
     *
     * @param schedule
     *
     * @throws DatabaseException
     */
    void create(Schedule schedule) throws DatabaseException;

    /**
     * Removes the given Schedule object from the database
     *
     * @param schedule
     *
     * @throws DatabaseException
     */
    void delete(Schedule schedule) throws DatabaseException;

    SingleColumnDatabaseDriver<Schedule, Cron> getFullCronDriver();

    SingleColumnDatabaseDriver<Schedule, Cron> getIncCronDriver();

    SingleColumnDatabaseDriver<Schedule, Integer> getKeepLocalDriver();

    SingleColumnDatabaseDriver<Schedule, Integer> getKeepRemoteDriver();

    SingleColumnDatabaseDriver<Schedule, OnFailure> getOnFailureDriver();

    SingleColumnDatabaseDriver<Schedule, Integer> getMaxRetriesDriver();

    StateFlagsPersistence<Schedule> getStateFlagsPersistence();
}
