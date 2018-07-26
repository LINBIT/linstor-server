package com.linbit.linstor.dbcp;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.core.LinStorArguments;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.GenericDbUtils;
import com.linbit.linstor.dbdrivers.H2DatabaseInfo;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Singleton;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_CONFIGURATION;

public class DbConnectionPoolModule extends AbstractModule
{
    // Database configuration file path
    private static final String DB_CONF_FILE = "database.cfg";

    // Database connection URL configuration key
    private static final String DB_CONN_URL = "connection-url";

    private static final String DEFAULT_DB_PATH = "/tmp/linstor";

    private static final String DERBY_CONNECTION_TEST_SQL =
        "SELECT 1 FROM " + TBL_SEC_CONFIGURATION;

    @Override
    protected void configure()
    {
        bind(ControllerDatabase.class).to(DbConnectionPool.class);
    }

    @Provides
    @Singleton
    public DbConnectionPool initializeDatabaseConnectionPool(
        ErrorReporter errorLogRef,
        LinStorArguments args
    )
        throws SQLException, InitializationException
    {
        errorLogRef.logInfo("Initializing the database connection pool");

        DbConnectionPool dbConnPool = new DbConnectionPool();

        Properties dbProps = loadDatabaseConfiguration(args);
        String connectionUrl = getConnectionUrl(errorLogRef, args, dbProps);
        String dbType = getDbType(connectionUrl);

        dbConnPool.initializeDataSource(
            connectionUrl,
            dbProps
        );

        dbConnPool.migrate(dbType);

        testDbConnection(dbConnPool);

        applyConfigurationArguments(args, dbConnPool);

        return dbConnPool;
    }

    private Properties loadDatabaseConfiguration(LinStorArguments args)
        throws InitializationException
    {
        Properties dbProps = new Properties();
        if (args.getInMemoryDbType() == null)
        {
            Path dbConfigFile = Paths.get(args.getConfigurationDirectory(), DB_CONF_FILE);
            try (InputStream dbPropsIn = new FileInputStream(dbConfigFile.toFile()))
            {
                dbProps.loadFromXML(dbPropsIn);
            }
            catch (IOException ioExc)
            {
                throw new InitializationException("Failed to load database configuration", ioExc);
            }
        }
        return dbProps;
    }

    private String getConnectionUrl(
        ErrorReporter errorLogRef,
        LinStorArguments args,
        Properties dbProps
    )
    {
        String connectionUrl;
        if (args.getInMemoryDbType() == null)
        {
            connectionUrl = dbProps.getProperty(
                DB_CONN_URL,
                new H2DatabaseInfo().jdbcUrl(DEFAULT_DB_PATH)
            );
        }
        else
        {
            errorLogRef.logInfo(
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

    private void testDbConnection(DbConnectionPool dbConnPool)
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

    private void applyConfigurationArguments(LinStorArguments args, DbConnectionPool dbConnPool)
        throws SQLException
    {
        if (args.getInMemoryDbType() != null)
        {
            Connection con = null;
            try
            {
                con = dbConnPool.getConnection();

                if (args.getInMemoryDbPort() > 0)
                {
                    GenericDbUtils.executeStatement(con,
                        String.format("UPDATE PROPS_CONTAINERS SET PROP_VALUE='%d' " +
                                "WHERE PROPS_INSTANCE='CTRLCFG' AND PROP_KEY='netcom/PlainConnector/port'",
                            args.getInMemoryDbPort()));
                }
                if (args.getInMemoryDbAddress() != null)
                {
                    GenericDbUtils.executeStatement(con,
                        String.format("UPDATE PROPS_CONTAINERS SET PROP_VALUE='%s' " +
                                "WHERE PROPS_INSTANCE='CTRLCFG' AND PROP_KEY='netcom/PlainConnector/bindaddress'",
                            args.getInMemoryDbAddress()));
                }
                con.commit();
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
