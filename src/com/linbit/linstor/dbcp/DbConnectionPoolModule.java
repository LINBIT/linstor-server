package com.linbit.linstor.dbcp;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.core.LinStorArguments;
import com.linbit.linstor.dbdrivers.DatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static com.linbit.linstor.dbdrivers.derby.DerbyConstants.TBL_SEC_CONFIGURATION;

public class DbConnectionPoolModule extends AbstractModule
{
    // Database configuration file path
    private static final String DB_CONF_FILE = "database.cfg";

    // Database connection URL configuration key
    private static final String DB_CONN_URL = "connection-url";

    private static final String DERBY_CONNECTION_TEST_SQL =
        "SELECT 1 FROM " + TBL_SEC_CONFIGURATION;

    @Override
    protected void configure()
    {
        bind(ControllerDatabase.class).to(DbConnectionPool.class);
    }

    private Properties loadDatabaseConfiguration(LinStorArguments args)
        throws InitializationException
    {
        Properties dbProps = new Properties();
        try (InputStream dbPropsIn = new FileInputStream(args.getWorkingDirectory() + DB_CONF_FILE))
        {
            dbProps.loadFromXML(dbPropsIn);
        }
        catch (IOException ioExc)
        {
            throw new InitializationException("Failed to load database configuration", ioExc);
        }
        return dbProps;
    }

    @Provides
    @Singleton
    public DbConnectionPool initializeDatabaseConnectionPool(
        ErrorReporter errorLogRef,
        LinStorArguments args,
        DatabaseDriver persistenceDbDriver
    )
        throws SQLException, InitializationException
    {
        errorLogRef.logInfo("Initializing the database connection pool");

        final Properties dbProps = loadDatabaseConfiguration(args);

        String connectionUrl = dbProps.getProperty(
            DB_CONN_URL,
            persistenceDbDriver.getDefaultConnectionUrl()
        );

        DbConnectionPool dbConnPool = new DbConnectionPool();

        // Connect the database connection pool to the database
        dbConnPool.initializeDataSource(
            connectionUrl,
            dbProps
        );

        // Test the database connection
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

        return dbConnPool;
    }

}
