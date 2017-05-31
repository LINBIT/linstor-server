package com.linbit.drbdmanage.dbdrivers;

import java.sql.SQLException;

import com.linbit.ServiceName;
import com.linbit.drbdmanage.dbcp.DbConnectionPool;
import com.linbit.drbdmanage.propscon.PropsConDatabaseDriver;
import com.linbit.drbdmanage.propscon.PropsContainer;

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
     * Returns the default connection URL for the driver's database type
     *
     * @return Default URL for connecting to the database
     */
    String getDefaultConnectionUrl();

    /**
     * Returns the database driver specific implementation for {@link PropsContainer}-IO.
     * @throws SQLException
     */
    PropsConDatabaseDriver getPropsConDatabaseDriver(String instanceName, DbConnectionPool dbPool) throws SQLException;
}
