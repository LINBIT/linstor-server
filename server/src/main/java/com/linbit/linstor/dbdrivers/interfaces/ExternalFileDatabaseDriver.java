package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

public interface ExternalFileDatabaseDriver
{
    /**
     * Creates or updates the given ExternalFile object into the database.
     *
     * @param file
     * @throws DatabaseException
     */
    void create(ExternalFile file) throws DatabaseException;

    /**
     * Removes the given DATA from the database
     *
     * @param file
     * @throws DatabaseException
     */
    void delete(ExternalFile file) throws DatabaseException;

    SingleColumnDatabaseDriver<ExternalFile, byte[]> getContentDriver();

    SingleColumnDatabaseDriver<ExternalFile, byte[]> getContentCheckSumDriver();

    StateFlagsPersistence<ExternalFile> getStateFlagPersistence();
}
