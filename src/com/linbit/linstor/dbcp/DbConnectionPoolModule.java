package com.linbit.linstor.dbcp;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.core.LinStorArguments;
import com.linbit.linstor.core.RecreateDb;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Singleton;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
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

    private static final String DEFAULT_DB_CONNECTION_URL = "jdbc:derby:directory:database";

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
        LinStorArguments args
    )
        throws SQLException, InitializationException
    {
        errorLogRef.logInfo("Initializing the database connection pool");

        DbConnectionPool dbConnPool = new DbConnectionPool();

        if (args.getMemoryDatabaseInitScript() == null)
        {
            final Properties dbProps = loadDatabaseConfiguration(args);

            String connectionUrl = dbProps.getProperty(
                DB_CONN_URL,
                DEFAULT_DB_CONNECTION_URL
            );

            // Connect the database connection pool to the database
            dbConnPool.initializeDataSource(
                connectionUrl,
                dbProps
            );
        }
        else
        {
            // In memory database
            // Connect the database connection pool to the database
            dbConnPool.initializeDataSource(
                "jdbc:derby:memory:testDb;create=True",
                new Properties()
            );

            Connection conn = null;
            try
            {
                String initSqlPath = args.getMemoryDatabaseInitScript();
                BufferedReader br = new BufferedReader(new FileReader(initSqlPath));
                conn = dbConnPool.getConnection();
                RecreateDb.runSql(conn, initSqlPath, br, false);
                br.close();
                errorLogRef.logInfo("Using in memory database with init script: " + initSqlPath);
            }
            catch (IOException ioerr)
            {
                errorLogRef.reportError(ioerr);
            }
            finally
            {
                if (conn != null)
                {
                    dbConnPool.returnConnection(conn);
                }
            }
        }

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
