package com.linbit.linstor.dbdrivers.interfaces.remotes;

import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import java.net.URL;
import java.util.UUID;

public interface LinstorRemoteDatabaseDriver
{
    /**
     * Creates or updates the given LinstorRemote object into the database.
     *
     * @param remote
     *
     * @throws DatabaseException
     */
    void create(LinstorRemote remote) throws DatabaseException;

    /**
     * Removes the given LinstorRemote object from the database
     *
     * @param remote
     *
     * @throws DatabaseException
     */
    void delete(LinstorRemote remote) throws DatabaseException;

    SingleColumnDatabaseDriver<LinstorRemote, URL> getUrlDriver();

    SingleColumnDatabaseDriver<LinstorRemote, byte[]> getEncryptedRemotePassphraseDriver();

    StateFlagsPersistence<LinstorRemote> getStateFlagsPersistence();

    SingleColumnDatabaseDriver<LinstorRemote, UUID> getClusterIdDriver();
}
