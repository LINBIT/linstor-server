package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Properties;
import com.linbit.SystemService;
import java.sql.Connection;

/**
 * Database access for the drbdmanage Controller module
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ControllerDatabase extends SystemService
{
    public static final int DEFAULT_TIMEOUT = 60000;
    public static final int DEFAULT_MAX_OPEN_STMT = 100;

    public static final String TBL_CTRL_CONF    = "CTRL_CONFIGURATION";

    public static final String CONF_KEY         = "ENTRY_KEY";
    public static final String CONF_DSP_KEY     = "ENTRY_DSP_KEY";
    public static final String CONF_VALUE       = "ENTRY_VALUE";

    void setTimeout(int timeout);
    void setMaxOpenPreparedStatements(int maxOpen);

    void initializeDataSource(String dbConnectionUrl, Properties props)
        throws SQLException;

    Connection getConnection() throws SQLException;

    // Must be able to handle dbConn == null as a valid input
    void returnConnection(Connection dbConn);

    @Override
    void shutdown();
}
