package com.linbit.linstor.dbcp;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.core.ControllerCmdlArguments;
import com.linbit.linstor.core.LinstorConfigToml;
import com.linbit.linstor.dbcp.migration.MigrationUtils;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;
import com.linbit.linstor.logging.ErrorReporter;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_CONFIGURATION;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;

public class DbConnectionPoolInitializer implements DbInitializer
{
    private static final String DERBY_CONNECTION_TEST_SQL =
        "SELECT 1 FROM " + TBL_SEC_CONFIGURATION;

    private final ErrorReporter errorLog;
    private final ControllerCmdlArguments args;
    private final DbConnectionPool dbConnPool;
    private final LinstorConfigToml linstorConfig;

    @Inject
    public DbConnectionPoolInitializer(
        ErrorReporter errorLogRef,
        ControllerCmdlArguments argsRef,
        ControllerDatabase dbConnPoolRef,
        LinstorConfigToml linstorConfigRef
    )
    {

        errorLog = errorLogRef;
        args = argsRef;
        dbConnPool = (DbConnectionPool) dbConnPoolRef;
        linstorConfig = linstorConfigRef;
    }

    public void initialize()
        throws InitializationException
    {
        errorLog.logInfo("Initializing the database connection pool");

        String connectionUrl = getConnectionUrl();
        String dbType = getDbType(connectionUrl);

        dbConnPool.initializeDataSource(connectionUrl);

        dbConnPool.migrate(dbType);

        testDbConnection();

        applyConfigurationArguments();
    }

    private String getConnectionUrl()
    {
        String connectionUrl;
        if (args.getInMemoryDbType() == null)
        {
            connectionUrl = linstorConfig.getDB().getConnectionUrl();
        }
        else
        {
            errorLog.logInfo(
                String.format("Using %s in memory database", args.getInMemoryDbType())
            );

            DatabaseDriverInfo dbInfo = DatabaseDriverInfo.createDriverInfo(args.getInMemoryDbType());
            connectionUrl = dbInfo.jdbcInMemoryUrl();
        }
        return connectionUrl;
    }

    private String getDbType(String connectionUrl)
        throws InitializationException
    {
        String dbType;
        String[] connectionUrlParts = connectionUrl.split(":");
        if (connectionUrlParts.length > 1)
        {
            dbType = connectionUrlParts[1];
        }
        else
        {
            throw new InitializationException("Failed to read DB type from connection URL");
        }
        return dbType;
    }

    private void testDbConnection()
        throws InitializationException
    {
        Connection conn = null;
        try
        {
            conn = dbConnPool.getConnection();
            conn.createStatement().executeQuery(DERBY_CONNECTION_TEST_SQL);
        }
        catch (SQLException exc)
        {
            throw new InitializationException("Failed to connect to database", exc);
        }
        finally
        {
            if (conn != null)
            {
                dbConnPool.returnConnection(conn);
            }
        }
    }

    private void applyConfigurationArguments()
        throws InitializationException
    {
        if (args.getInMemoryDbType() != null)
        {
            Connection con = null;
            try
            {
                con = dbConnPool.getConnection();

                if (args.getInMemoryDbPort() > 0)
                {
                    SQLUtils.executeStatement(con,
                        String.format("UPDATE PROPS_CONTAINERS SET PROP_VALUE='%d' " +
                                "WHERE PROPS_INSTANCE='/CTRLCFG' AND PROP_KEY='netcom/PlainConnector/port'",
                            args.getInMemoryDbPort()));
                }
                if (args.getInMemoryDbAddress() != null)
                {
                    SQLUtils.executeStatement(con,
                        String.format("UPDATE PROPS_CONTAINERS SET PROP_VALUE='%s' " +
                                "WHERE PROPS_INSTANCE='/CTRLCFG' AND PROP_KEY='netcom/PlainConnector/bindaddress'",
                            args.getInMemoryDbAddress()));
                }
                con.commit();
            }
            catch (SQLException exc)
            {
                throw new InitializationException("Failed to update database with configuration from arguments");
            }
            finally
            {
                if (con != null)
                {
                    dbConnPool.returnConnection(con);
                }
            }
        }
    }
}
