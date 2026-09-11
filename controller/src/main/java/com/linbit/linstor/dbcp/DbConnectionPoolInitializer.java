package com.linbit.linstor.dbcp;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.H2FormatUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_PROPS_CONTAINERS;

import jakarta.inject.Inject;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class DbConnectionPoolInitializer implements DbInitializer
{
    private static final String DERBY_CONNECTION_TEST_SQL =
        "SELECT 1 FROM " + TBL_PROPS_CONTAINERS;

    private final ErrorReporter errorLog;
    private final DbConnectionPool dbConnPool;
    private final CtrlConfig ctrlCfg;

    private boolean enableMigrationOnInit = true;

    @Inject
    public DbConnectionPoolInitializer(
        ErrorReporter errorLogRef,
        ControllerDatabase dbConnPoolRef,
        CtrlConfig ctrlCfgRef
    )
    {

        errorLog = errorLogRef;
        dbConnPool = (DbConnectionPool) dbConnPoolRef;
        ctrlCfg = ctrlCfgRef;
    }

    @Override
    public void setEnableMigrationOnInit(boolean enableRef)
    {
        enableMigrationOnInit = enableRef;
    }

    @Override
    public void initialize() throws InitializationException, SystemServiceStartException
    {
        errorLog.logInfo("Initializing the database connection pool");

        try
        {
            String connectionUrl = getConnectionUrl();
            String dbType = getDbType(connectionUrl);

            checkLegacyH2Db(connectionUrl);
            dbConnPool.initializeDataSource(connectionUrl);
            if (enableMigrationOnInit)
            {
                var dbInfo = DatabaseDriverInfo.createDriverInfo(dbType);
                dbConnPool.migrate(dbInfo, !ctrlCfg.isDbVersionCheckDisabled());

                // testDbConnection requires a "LINSTOR" schema, which only exists after migration is done.
                testDbConnection();
            }
        }
        catch (Exception exc)
        {
            throw new SystemServiceStartException("Database initialization error", exc, true);
        }
    }

    @Override
    public boolean needsMigration() throws DatabaseException, InitializationException
    {
        String connectionUrl = getConnectionUrl();
        String dbType = getDbType(connectionUrl);

        dbConnPool.initializeDataSource(connectionUrl);
        return dbConnPool.needsMigration(dbType);
    }

    @Override
    public void migrateTo(Object versionRef) throws DatabaseException, InitializationException
    {
        final String connectionUrl = getConnectionUrl();
        final String dbType = getDbType(connectionUrl);

        errorLog.logInfo("Migrating to version \"%s\", using JDBC: \"%s\"", versionRef, connectionUrl);
        dbConnPool.initializeDataSource(connectionUrl);
        dbConnPool.preImportMigrateToVersion(dbType, versionRef);
    }

    /**
     * The H2 2.x driver can neither read nor upgrade database files written by H2 1.x, its own error
     * would be a rather cryptic "Unsupported database file version". Fail with a clear message instead.
     * When the H2 1.x driver is loaded (as done by the export phase of "linstor-database migrate-h2"),
     * this check is a no-op.
     */
    private void checkLegacyH2Db(String connectionUrl) throws InitializationException
    {
        try
        {
            if (H2FormatUtils.loadedH2MajorVersion() >= 2)
            {
                @Nullable Path legacyDbFile = H2FormatUtils.findLegacyH2DbFile(connectionUrl);
                if (legacyDbFile != null)
                {
                    throw new InitializationException(
                        "The database file " + legacyDbFile + " was written by H2 1.x and cannot be " +
                            "opened with the H2 " + H2FormatUtils.loadedH2MajorVersion() + ".x driver. " +
                            "Run 'linstor-database migrate-h2' to migrate the database to the new format."
                    );
                }
            }
        }
        catch (IOException exc)
        {
            throw new InitializationException("Failed to check the H2 database file format", exc);
        }
    }

    private String getConnectionUrl()
    {
        String connectionUrl;
        String inMemoryDb = ctrlCfg.getDbInMemory();
        if (inMemoryDb == null)
        {
            connectionUrl = ctrlCfg.getDbConnectionUrl();
        }
        else
        {
            errorLog.logInfo(
                String.format("Using %s in memory database", inMemoryDb)
            );

            DatabaseDriverInfo dbInfo = DatabaseDriverInfo.createDriverInfo(inMemoryDb);
            connectionUrl = dbInfo.jdbcInMemoryUrl();
        }
        return connectionUrl;
    }

    public static String getDbType(String connectionUrl)
        throws InitializationException
    {
        String dbType;
        String[] connectionUrlParts = StringUtils.split(connectionUrl, ":");
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

    @Override
    public void shutdown(boolean jvmShutdownRef)
    {
        dbConnPool.shutdown(jvmShutdownRef);
    }

    @Override
    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        dbConnPool.awaitShutdown(timeoutRef);
    }

    @SuppressFBWarnings("ODR_OPEN_DATABASE_RESOURCE_EXCEPTION_PATH")
    private void testDbConnection()
        throws InitializationException
    {
        Connection conn = null;
        try
        {
            conn = dbConnPool.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(DERBY_CONNECTION_TEST_SQL);
            rs.close();
            stmt.close();
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
}
