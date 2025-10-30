package com.linbit.linstor.dbcp;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerSQLDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.ClassPathLoader;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbcp.migration.LinstorMigration;
import com.linbit.linstor.dbcp.migration.Migration;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.DATABASE_SCHEMA_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_CONFIGURATION;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * JDBC pool
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class DbConnectionPool implements ControllerSQLDatabase
{
    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "SQL database connection pool service";

    private int dbTimeout = ControllerDatabase.DEFAULT_TIMEOUT;
    private int dbMaxOpen = ControllerSQLDatabase.DEFAULT_MAX_OPEN_STMT;

    private static final int DEFAULT_MIN_IDLE_CONNECTIONS =  10;
    private static final int DEFAULT_MAX_IDLE_CONNECTIONS = 100;
    private static final int DEFAULT_IDLE_TIMEOUT = 1 * 60 * 60 * 1000; // 1 hour in ms

    private @Nullable PoolingDataSource<PoolableConnection> dataSource = null;

    private ServiceName serviceNameInstance;
    private @Nullable String dbConnectionUrl;
    private final AtomicBoolean atomicStarted = new AtomicBoolean(false);

    private final ThreadLocal<List<Connection>> threadLocalConnections;

    private final CtrlConfig linstorConfig;
    private final ErrorReporter errorLog;

    static
    {
        try
        {
            SERVICE_NAME = new ServiceName("DatabaseService");
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(invalidNameExc);
        }
    }

    @Inject
    public DbConnectionPool(
        CtrlConfig linstorConfigRef,
        ErrorReporter errorLogRef
    )
    {
        serviceNameInstance = SERVICE_NAME;
        threadLocalConnections = new ThreadLocal<>();
        linstorConfig = linstorConfigRef;
        errorLog = errorLogRef;
    }

    @Override
    public void setTimeout(int timeout)
    {
        if (timeout < 0)
        {
            throw new ImplementationError(
                "Attempt to set the database timeout to less than zero",
                new ValueOutOfRangeException(ValueOutOfRangeException.ViolationType.TOO_LOW)
            );
        }
        dbTimeout = timeout;
    }

    @Override
    public void setMaxOpenPreparedStatements(int maxOpen)
    {
        if (maxOpen < 0)
        {
            throw new ImplementationError(
                "Attempt to set the database max open statements to less than zero",
                new ValueOutOfRangeException(ValueOutOfRangeException.ViolationType.TOO_LOW)
            );
        }
        dbMaxOpen = maxOpen;
    }

    @Override
    public void initializeDataSource(String dbConnectionUrlRef)
    {
        ErrorCheck.ctorNotNull(DbConnectionPool.class, String.class, dbConnectionUrlRef);
        dbConnectionUrl = dbConnectionUrlRef;

        if (isH2DataSource())
        {
            dbConnectionUrl += ";DB_CLOSE_ON_EXIT=FALSE";
        }

        errorLog.logInfo("SQL database connection URL is \"%s\"", dbConnectionUrl);

        try
        {
            start();
        }
        catch (SystemServiceStartException systemServiceStartExc)
        {
            throw new ImplementationError(systemServiceStartExc);
        }
    }

    @Override
    public Connection getConnection()
        throws SQLException
    {
        @Nullable Connection dbConn = null;
        if (dataSource != null)
        {
            dbConn = dataSource.getConnection();
            @Nullable List<Connection> connections = threadLocalConnections.get();
            if (connections == null)
            {
                connections = new ArrayList<>();
                threadLocalConnections.set(connections);
            }
            connections.add(dbConn);
            dbConn.setSchema(DATABASE_SCHEMA_NAME);
            // setSchema must be called before we enable explicit transactions with setAutoCommit(false):
            // In postgresql SET SCHEMA already "activates" the transaction and could collide with other
            // getConnection() attempts and so we should keep the below order:
            // t1: any ApiCall
            // t1: (auto-start tx;) set schema; (auto-commit;)
            // t1: (implicit start tx;) insert...
            // t2: nodeupdate ApiCall
            // t2: (auto-start tx;) set schema; (auto-commit;)

            // t1: commit tx;
            // t2: (implicit start tx;) insert ...
            // Otherwise, postgresql might throw such an error:
            // could not serialize access due to read/write dependencies among transactions

            dbConn.setAutoCommit(false);
        }

        if (dbConn == null)
        {
            System.err.println("Error: Unable to get Database connection.");
            System.exit(InternalApiConsts.EXIT_CODE_IMPL_ERROR);
        }
        return dbConn;
    }

    @Override
    public void returnConnection(@Nullable Connection dbConn)
    {
        try
        {
            if (dbConn != null)
            {
                dbConn.close();
                List<Connection> list = threadLocalConnections.get();
                if (list != null)
                {
                    list.remove(dbConn);
                }
            }
        }
        catch (SQLException ignored)
        {
        }
    }

    @Override
    public void migrate(String dbType) throws InitializationException
    {
        var dbMigrater = new DbMigrater(errorLog);
        var dbInfo = DatabaseDriverInfo.createDriverInfo(dbType);
        try (Connection conn = dataSource.getConnection())
        {
            dbMigrater.migrate(conn, dbInfo, false);
        }
        catch (SQLException exc)
        {
            throw new InitializationException(exc);
        }
    }

    public void migrate(DatabaseDriverInfo dbInfo, boolean withStartupVer)
        throws InitializationException
    {
        var dbMigrater = new DbMigrater(errorLog);
        try (Connection conn = dataSource.getConnection())
        {
            dbMigrater.migrate(conn, dbInfo, withStartupVer);
        }
        catch (SQLException exc)
        {
            throw new InitializationException(exc);
        }
    }

    public String getCurrentVersion()
    {
        // DO NOT use Flyway, since it requires an actual connection to the database, which might not be running if we
        // are using ETCD or K8s

        ClassPathLoader classPathLoader = new ClassPathLoader(errorLog);
        List<Class<? extends LinstorMigration>> sqlMigrationClasses = classPathLoader.loadClasses(
            LinstorMigration.class.getPackage().getName(),
            Collections.singletonList(""),
            LinstorMigration.class,
            Migration.class
        );

        TreeSet<String> versions = new TreeSet<>();
        for (Class<? extends LinstorMigration> migrationClass : sqlMigrationClasses)
        {
            Migration[] annotations = migrationClass.getAnnotationsByType(Migration.class);
            versions.add(annotations[0].version());
        }

        return versions.last();
    }

    @Override
    public void preImportMigrateToVersion(String dbTypeRef, Object versionRef)
        throws InitializationException, DatabaseException
    {
        var dbMigrater = new DbMigrater(errorLog);
        var dbInfo = DatabaseDriverInfo.createDriverInfo(dbTypeRef);
        try (Connection conn = dataSource.getConnection())
        {
            dbMigrater.setSchema(conn, dbInfo);
            dbMigrater.migrateToVersion(conn, dbInfo, (String) versionRef);
        }
        catch (SQLException exc)
        {
            throw new DatabaseException(exc);
        }
    }

    @Override
    public boolean needsMigration(String dbType)
    {
        var dbMigrater = new DbMigrater(errorLog);
        try (Connection conn = getConnection())
        {
            return dbMigrater.needsMigration(conn, dbType);
        }
        catch (SQLException sqlExc)
        {
            errorLog.logError("Error getting db connection: " + sqlExc);
        }
        return true;
    }

    @Override
    public boolean closeAllThreadLocalConnections()
    {
        boolean ret = false;
        List<Connection> list = threadLocalConnections.get();
        if (list != null)
        {
            for (Connection conn : list)
            {
                try
                {
                    ret |= !conn.isClosed();
                    conn.close();
                }
                catch (SQLException ignored)
                {
                }
            }
            list.clear();
        }
        return ret;
    }


    @Override
    public void shutdown(boolean jvmShutdownRef)
    {
        try
        {
            if (jvmShutdownRef && isH2DataSource())
            {
                try (
                    Connection con = getConnection();
                    Statement stmt = con.createStatement()
                )
                {
                    stmt.execute("SHUTDOWN");
                }
            }
            dataSource.close();
            atomicStarted.set(false);
        }
        catch (Exception exc)
        {
            // FIXME: report using the Controller's ErrorReporter instance
        }
    }

    private boolean isH2DataSource()
    {
        return dbConnectionUrl.startsWith("jdbc:h2");
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceName)
    {
        if (instanceName == null)
        {
            serviceNameInstance = SERVICE_NAME;
        }
        else
        {
            serviceNameInstance = instanceName;
        }
    }

    @Override
    public void start() throws SystemServiceStartException
    {
        if (!atomicStarted.getAndSet(true))
        {
            Properties props = new Properties();
            if (linstorConfig.getDbUser() != null)
            {
                props.setProperty("user", linstorConfig.getDbUser());
            }
            if (linstorConfig.getDbPassword() != null)
            {
                props.setProperty("password", linstorConfig.getDbPassword());
            }
            ConnectionFactory connFactory = new DriverManagerConnectionFactory(dbConnectionUrl, props);
            PoolableConnectionFactory poolConnFactory = new PoolableConnectionFactory(connFactory, null);

            GenericObjectPoolConfig<PoolableConnection> poolConfig = new GenericObjectPoolConfig<>();
            poolConfig.setMinIdle(DEFAULT_MIN_IDLE_CONNECTIONS);
            poolConfig.setMaxIdle(DEFAULT_MAX_IDLE_CONNECTIONS);
            poolConfig.setBlockWhenExhausted(true);
            poolConfig.setFairness(true);
            GenericObjectPool<PoolableConnection> connPool = new GenericObjectPool<>(poolConnFactory, poolConfig);

            poolConnFactory.setPool(connPool);
            poolConnFactory.setValidationQueryTimeout(dbTimeout);
            poolConnFactory.setMaxOpenPreparedStatements(dbMaxOpen);
            poolConnFactory.setMaxConnLifetimeMillis(DEFAULT_IDLE_TIMEOUT);
            poolConnFactory.setDefaultTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            dataSource = new PoolingDataSource<>(connPool);
        }
    }

    @Override
    public void awaitShutdown(long timeout) throws InterruptedException
    {
        // no await time
    }

    @Override
    public ServiceName getServiceName()
    {
        return SERVICE_NAME;
    }

    @Override
    public String getServiceInfo()
    {
        return SERVICE_INFO;
    }

    @Override
    public ServiceName getInstanceName()
    {
        return serviceNameInstance;
    }

    @Override
    public boolean isStarted()
    {
        return atomicStarted.get();
    }

    @SuppressFBWarnings("ODR_OPEN_DATABASE_RESOURCE_EXCEPTION_PATH")
    @Override
    public void checkHealth() throws DatabaseException
    {
        Connection conn = null;
        try
        {
            conn = getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 FROM " + TBL_SEC_CONFIGURATION);
            rs.close();
            stmt.close();
        }
        catch (SQLException exc)
        {
            throw new DatabaseException(exc);
        }
        finally
        {
            // this method also closes the connection
            returnConnection(conn);
        }
    }
}
