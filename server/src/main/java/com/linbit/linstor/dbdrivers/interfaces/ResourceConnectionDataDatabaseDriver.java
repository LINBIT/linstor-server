package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.ResourceConnectionData;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link ResourceConnectionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface ResourceConnectionDataDatabaseDriver
{
    /**
     * Persists the given {@link ResourceConnectionData} into the database.
     *
     * @param resConDfnData
     *  The data to be stored (including the primary key)
     * @throws DatabaseException
     */
    void create(ResourceConnectionData resConDfnData) throws DatabaseException;

    /**
     * Removes the given {@link ResourceConnectionData} from the database
     *
     * @param resConDfnData
     *  The data identifying the database entry to delete
     * @throws DatabaseException
     */
    void delete(ResourceConnectionData resConDfnData) throws DatabaseException;

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<ResourceConnectionData> getStateFlagPersistence();

    /**
     * A special sub-driver to update the port
     */
    SingleColumnDatabaseDriver<ResourceConnectionData, TcpPortNumber> getPortDriver();
}
