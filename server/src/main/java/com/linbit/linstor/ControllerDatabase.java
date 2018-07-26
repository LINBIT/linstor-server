package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Properties;
import com.linbit.SystemService;

import java.sql.Connection;

/**
 * Database access for the linstor Controller module
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ControllerDatabase extends SystemService
{
    int DEFAULT_TIMEOUT = 60000;
    int DEFAULT_MAX_OPEN_STMT = 100;

    void setTimeout(int timeout);
    void setMaxOpenPreparedStatements(int maxOpen);

    void initializeDataSource(String dbConnectionUrl, Properties props)
        throws SQLException;

    Connection getConnection() throws SQLException;

    // Must be able to handle dbConn == null as a valid input
    void returnConnection(Connection dbConn);

    void migrate(String dbType);

    @Override
    void shutdown();
}
