package com.linbit.linstor.dbcp;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerSQLDatabase;
import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.core.LinstorConfigToml;
import com.linbit.linstor.dbcp.migration.LinstorMigration;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.SQLUtils;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.DatabaseInfo.DB2_MIN_VERSION;
import static com.linbit.linstor.DatabaseInfo.DERBY_MIN_VERSION;
import static com.linbit.linstor.DatabaseInfo.H2_MIN_VERSION;
import static com.linbit.linstor.DatabaseInfo.INFORMIX_MIN_VERSION;
import static com.linbit.linstor.DatabaseInfo.MARIADB_MIN_VERSION;
import static com.linbit.linstor.DatabaseInfo.MYSQL_MIN_VERSION;
import static com.linbit.linstor.DatabaseInfo.POSTGRES_MIN_VERSION;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.DATABASE_SCHEMA_NAME;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_CONFIGURATION;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.Configuration;

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
    private static final String SCHEMA_HISTORY_TABLE_NAME = "FLYWAY_SCHEMA_HISTORY";

    private int dbTimeout = ControllerDatabase.DEFAULT_TIMEOUT;
    private int dbMaxOpen = ControllerSQLDatabase.DEFAULT_MAX_OPEN_STMT;

    private static final int DEFAULT_MIN_IDLE_CONNECTIONS =  10;
    private static final int DEFAULT_MAX_IDLE_CONNECTIONS = 100;
    private static final int DEFAULT_IDLE_TIMEOUT = 1 * 60 * 60 * 1000; // 1 hour in ms

    private PoolingDataSource<PoolableConnection> dataSource = null;

    private ServiceName serviceNameInstance;
    private String dbConnectionUrl;
    private AtomicBoolean atomicStarted = new AtomicBoolean(false);

    private ThreadLocal<List<Connection>> threadLocalConnections;

    private final LinstorConfigToml linstorConfig;

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
        LinstorConfigToml linstorConfigRef
    )
    {
        serviceNameInstance = SERVICE_NAME;
        threadLocalConnections = new ThreadLocal<>();
        linstorConfig = linstorConfigRef;
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
        Connection dbConn = null;
        if (dataSource != null)
        {
            dbConn = dataSource.getConnection();
            List<Connection> connections = threadLocalConnections.get();
            if (connections == null)
            {
                connections = new ArrayList<>();
                threadLocalConnections.set(connections);
            }
            connections.add(dbConn);
            dbConn.setAutoCommit(false);
            dbConn.setSchema(DATABASE_SCHEMA_NAME);
        }

        if (dbConn == null)
        {
            System.err.println("Error: Unable to get Database connection.");
            System.exit(InternalApiConsts.EXIT_CODE_IMPL_ERROR);
        }
        return dbConn;
    }

    @Override
    public void returnConnection(Connection dbConn)
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
        migrate(dbType, false);
    }

    public void migrate(String dbType, boolean withStartupVer) throws InitializationException
    {
        setTransactionIsolation(dbType);

        if (withStartupVer)
        {
            checkMinVersion();
        }

        Flyway.configure()
            .schemas(DATABASE_SCHEMA_NAME)
            .dataSource(dataSource)
            .table(SCHEMA_HISTORY_TABLE_NAME)
            // When migrations are added in branches they can be applied in different orders
            .outOfOrder(true)
            // Pass the DB type to the migrations
            .placeholders(ImmutableMap.of(LinstorMigration.PLACEHOLDER_KEY_DB_TYPE, dbType))
            .locations(LinstorMigration.class.getPackage().getName())
            .load()
            .migrate();
    }

    private void checkMinVersion() throws InitializationException
    {
        try
        {
            DatabaseMetaData databaseMetaData = new org.flywaydb.core.api.migration.Context()
            {
                @Override
                public Configuration getConfiguration()
                {
                    return null;
                }

                @Override
                public Connection getConnection()
                {
                    Connection ret;
                    try
                    {
                        ret = dataSource.getConnection();
                    }
                    catch (SQLException sqlExc)
                    {
                        throw new LinStorDBRuntimeException("Failed to set transaction isolation", sqlExc);
                    }
                    return ret;
                }
            }
                .getConnection().getMetaData();

            String dbProductName = databaseMetaData.getDatabaseProductName();
            String dbProductVersion = databaseMetaData.getDatabaseProductVersion();

            // check if minimum version requirements of certain databases are satisfied
            int[] dbProductMinVersion = null;
            switch (DatabaseInfo.getDbProduct(dbProductName, dbProductVersion))
            {
                case H2:
                    dbProductMinVersion = H2_MIN_VERSION;
                    break;
                case DERBY:
                    dbProductMinVersion = DERBY_MIN_VERSION;
                    break;
                case DB2:
                    dbProductMinVersion = DB2_MIN_VERSION;
                    break;
                case POSTGRESQL:
                    dbProductMinVersion = POSTGRES_MIN_VERSION;
                    break;
                case MYSQL:
                    dbProductMinVersion = MYSQL_MIN_VERSION;
                    break;
                case MARIADB:
                    dbProductMinVersion = MARIADB_MIN_VERSION;
                    break;
                case INFORMIX:
                    dbProductMinVersion = INFORMIX_MIN_VERSION;
                    break;
                case ASE: // fall-through
                case DB2_I: // fall-through
                case DB2_Z: // fall-through
                case ETCD: // fall-through
                case MSFT_SQLSERVER: // fall-through
                case ORACLE_RDBMS: // fall-through
                case UNKNOWN: // fall-through
                default:
                    // currently no other databases with minimum version requirement
                    break;
            }

            if (dbProductMinVersion != null)
            {
                final String[] versionNumberSplit = dbProductVersion.split("\\s");
                if (versionNumberSplit.length > 0)
                {
                    String[] currVersionSplit = versionNumberSplit[0].split("\\.");
                    int currVersionMajor = Integer.parseInt(currVersionSplit[0]);
                    int currVersionMinor = Integer.parseInt(currVersionSplit[1]);
                    int minVersionMajor = dbProductMinVersion[0];
                    int minVersionMinor = dbProductMinVersion[1];

                    if (
                        currVersionMajor < minVersionMajor ||
                            currVersionMajor == minVersionMajor && currVersionMinor < minVersionMinor
                    )
                    {
                        throw new InitializationException(
                            StringUtils.join(
                                "",
                                "Currently installed version (",
                                currVersionMajor + "." + currVersionMinor,
                                ") of database '", dbProductName,
                                "' is older than the required minimum version (",
                                minVersionMajor + "." + minVersionMinor, ")!"
                            )
                        );
                    }
                    // else: everything is fine so we can proceed with the migration process
                }
                else
                {
                    throw new InitializationException(
                        "Failed to verify minimal database version! You can try to run linstor-controller without " +
                        "database version check"
                    );
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new InitializationException("Failed to verify minimal database version!", sqlExc);
        }
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
    public void shutdown()
    {
        try
        {
            dataSource.close();
            atomicStarted.set(false);
        }
        catch (Exception exc)
        {
            // FIXME: report using the Controller's ErrorReporter instance
        }
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
            if (linstorConfig.getDB().getUser() != null)
            {
                props.setProperty("user", linstorConfig.getDB().getUser());
            }
            if (linstorConfig.getDB().getPassword() != null)
            {
                props.setProperty("password", linstorConfig.getDB().getPassword());
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

            dataSource = new PoolingDataSource<PoolableConnection>(connPool);
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

    @Override
    public void checkHealth() throws DatabaseException
    {
        Connection conn = null;
        try
        {
            conn = getConnection();
            conn.createStatement().executeQuery("SELECT 1 FROM " + TBL_SEC_CONFIGURATION);
        }
        catch (SQLException exc)
        {
            throw new DatabaseException(exc);
        }
        finally
        {
            if (conn != null)
            {
                returnConnection(conn);
            }
        }
    }

    private void setTransactionIsolation(String dbType)
    {
        try
        {
            try (Connection connection = dataSource.getConnection())
            {
                DatabaseDriverInfo databaseInfo = DatabaseDriverInfo.createDriverInfo(dbType);
                SQLUtils.executeStatement(connection, databaseInfo.isolationStatement());
            }
        }
        catch (SQLException exc)
        {
            throw new LinStorDBRuntimeException("Failed to set transaction isolation", exc);
        }
    }
}
