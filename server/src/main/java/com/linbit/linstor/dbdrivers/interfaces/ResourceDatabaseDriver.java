package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link Resource}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface ResourceDatabaseDriver extends AbsResourceDatabaseDriver<Resource>
{
    /**
     * Persists the given {@link Resource} into the database.
     *
     * The primary key for the insert statement is stored as
     * instance variables already, thus might not be retrieved from the
     * conDfnData parameter.
     *
     * @param resource
     *  The data to be stored (including the primary key)
     *
     * @throws DatabaseException
     */
    void create(AbsResource<Resource> resource) throws DatabaseException;

    /**
     * Removes the given {@link Resource} from the database.
     *
     * @param resource
     *  The data identifying the row to delete
     *
     * @throws DatabaseException
     */
    void delete(AbsResource<Resource> resource) throws DatabaseException;

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<AbsResource<Resource>> getStateFlagPersistence();
}
