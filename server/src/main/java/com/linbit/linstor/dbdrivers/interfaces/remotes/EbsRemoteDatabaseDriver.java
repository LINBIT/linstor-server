package com.linbit.linstor.dbdrivers.interfaces.remotes;

import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import java.net.URL;

public interface EbsRemoteDatabaseDriver
{
    /**
     * Creates or updates the given EbsRemote object into the database.
     *
     * @param remote
     *
     * @throws DatabaseException
     */
    void create(EbsRemote remote) throws DatabaseException;

    /**
     * Removes the given EbsRemote object from the database
     *
     * @param remote
     *
     * @throws DatabaseException
     */
    void delete(EbsRemote remote) throws DatabaseException;

    SingleColumnDatabaseDriver<EbsRemote, URL> getUrlDriver();

    SingleColumnDatabaseDriver<EbsRemote, String> getAvailabilityZoneDriver();

    SingleColumnDatabaseDriver<EbsRemote, String> getRegionDriver();

    SingleColumnDatabaseDriver<EbsRemote, byte[]> getEncryptedSecretKeyDriver();

    SingleColumnDatabaseDriver<EbsRemote, byte[]> getEncryptedAccessKeyDriver();

    StateFlagsPersistence<EbsRemote> getStateFlagsPersistence();
}
