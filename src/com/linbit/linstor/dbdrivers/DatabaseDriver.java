package com.linbit.linstor.dbdrivers;

import com.linbit.ServiceName;
import com.linbit.TransactionMgr;
import com.linbit.linstor.ResourceDefinition;

import java.sql.SQLException;

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
     * Fetches all {@link com.linbit.linstor.Node}s, {@link ResourceDefinition}s and
     * {@link com.linbit.linstor.StorPoolDefinition}s from
     * the database.
     *
     * @param transMgr
     *
     * @throws SQLException
     */
    void loadAll(TransactionMgr transMgr) throws SQLException;
}
