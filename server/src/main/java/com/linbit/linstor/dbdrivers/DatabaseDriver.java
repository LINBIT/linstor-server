package com.linbit.linstor.dbdrivers;

import com.linbit.ServiceName;
import com.linbit.linstor.core.objects.ResourceDefinition;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface DatabaseDriver
{
    /**
     * Returns the default service name for a database service instance using this driver
     */
    ServiceName getDefaultServiceInstanceName();

    /**
     * Fetches all {@link com.linbit.linstor.core.objects.Node}s, {@link ResourceDefinition}s and
     * {@link com.linbit.linstor.core.objects.StorPoolDefinition}s from
     * the database.
     *
     * @param transMgr
     *
     * @throws DatabaseException
     */
    void loadAll() throws DatabaseException;
}
