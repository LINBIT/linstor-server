package com.linbit.linstor.dbcp;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.core.LinStorArguments;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.DerbyDriver;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Singleton;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Collectors;

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

        if (args.getInMemoryDbType() == null)
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
            Properties props = new Properties();

            /*
             * WORKAROUND: Set the default SCHEMA for all connections.
             * If no SCHEMA is defined, the user's name will be taken as default SCHEMA.
             * The first connection does not need a SCHEMA to work properly.
             * However, this changes with nested connections (opening a connection while an other
             * is not closed yet).
             * Without specifying a SCHEMA (in this case it is done by setting the db user
             * to 'LINSTOR'), the nested connection will not find anything in the database, not
             * even the tables or views and throws an SQLException.
             */
            props.setProperty("user", "LINSTOR");

            DatabaseDriverInfo dbInfo = DatabaseDriverInfo.CreateDriverInfo(args.getInMemoryDbType());
            dbConnPool.initializeDataSource(
                dbInfo.jdbcInMemoryUrl(),
                props
            );

            Connection con = null;
            try
            {
                con = dbConnPool.getConnection();
                InputStream is = DbConnectionPoolModule.class.getResourceAsStream("/resource/drbd-init-derby.sql");
                if (is == null)
                {
                    is = Files.newInputStream(Paths.get("./sql-src/drbd-init-derby.sql"));
                }

                try (BufferedReader br = new BufferedReader(new InputStreamReader(is)))
                {
                    DerbyDriver.executeStatement(con, dbInfo.isolationStatement());
                    DerbyDriver.runSql(
                        con,
                        dbInfo.prepareInit(br.lines().collect(Collectors.joining("\n")))
                    );
                }

                if (args.getInMemoryDbPort() > 0)
                {
                    DerbyDriver.executeStatement(con,
                        String.format("UPDATE PROPS_CONTAINERS SET PROP_VALUE='%d' "
                            + "WHERE PROPS_INSTANCE='CTRLCFG' AND PROP_KEY='netcom/PlainConnector/port'",
                            args.getInMemoryDbPort()));
                }
                if (args.getInMemoryDbAddress() != null)
                {
                    DerbyDriver.executeStatement(con,
                        String.format("UPDATE PROPS_CONTAINERS SET PROP_VALUE='%s' "
                                + "WHERE PROPS_INSTANCE='CTRLCFG' AND PROP_KEY='netcom/PlainConnector/bindaddress'",
                            args.getInMemoryDbAddress()));
                }
                con.commit();
                errorLogRef.logInfo(
                    String.format("Using %s in memory database with init script: ", args.getInMemoryDbType())
                );
            }
            catch (IOException ioerr)
            {
                errorLogRef.reportError(ioerr);
            }
            finally
            {
                if (con != null)
                {
                    dbConnPool.returnConnection(con);
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
