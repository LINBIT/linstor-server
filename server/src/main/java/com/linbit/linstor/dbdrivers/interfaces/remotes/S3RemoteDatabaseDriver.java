package com.linbit.linstor.dbdrivers.interfaces.remotes;

import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

public interface S3RemoteDatabaseDriver
{
    /**
     * Creates or updates the given S3Remote object into the database.
     *
     * @param remote
     *
     * @throws DatabaseException
     */
    void create(S3Remote remote) throws DatabaseException;

    /**
     * Removes the given S3Remote object from the database
     *
     * @param remote
     *
     * @throws DatabaseException
     */
    void delete(S3Remote remote) throws DatabaseException;

    SingleColumnDatabaseDriver<S3Remote, String> getEndpointDriver();

    SingleColumnDatabaseDriver<S3Remote, String> getBucketDriver();

    SingleColumnDatabaseDriver<S3Remote, String> getRegionDriver();

    SingleColumnDatabaseDriver<S3Remote, byte[]> getAccessKeyDriver();

    SingleColumnDatabaseDriver<S3Remote, byte[]> getSecretKeyDriver();

    StateFlagsPersistence<S3Remote> getStateFlagsPersistence();
}
