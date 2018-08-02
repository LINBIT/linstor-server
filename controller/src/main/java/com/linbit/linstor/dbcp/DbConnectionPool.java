package com.linbit.linstor.dbcp;

import com.google.common.collect.ImmutableMap;
import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.dbcp.migration.LinstorMigration;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.flywaydb.core.Flyway;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.DATABASE_SCHEMA_NAME;

/**
 * JDBC pool
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class DbConnectionPool implements ControllerDatabase
{
    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "SQL database connection pool service";
    private static final String SCHEMA_HISTORY_TABLE_NAME = "FLYWAY_SCHEMA_HISTORY";

    private int dbTimeout = ControllerDatabase.DEFAULT_TIMEOUT;
    private int dbMaxOpen = ControllerDatabase.DEFAULT_MAX_OPEN_STMT;

    public static final int DEFAULT_MIN_IDLE_CONNECTIONS =  10;
    public static final int DEFAULT_MAX_IDLE_CONNECTIONS = 100;

    private int minIdleConnections = DEFAULT_MIN_IDLE_CONNECTIONS;
    private int maxIdleConnections = DEFAULT_MAX_IDLE_CONNECTIONS;

    private PoolingDataSource<PoolableConnection> dataSource = null;

    private ServiceName serviceNameInstance;
    private String dbConnectionUrl;
    private Properties props;
    private AtomicBoolean atomicStarted = new AtomicBoolean(false);

    private ThreadLocal<List<Connection>> threadLocalConnections;

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
    public DbConnectionPool()
    {
        serviceNameInstance = SERVICE_NAME;
        threadLocalConnections = new ThreadLocal<>();
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
    public void initializeDataSource(String dbConnectionUrlRef, Properties propsRef)
    {
        ErrorCheck.ctorNotNull(DbConnectionPool.class, String.class, dbConnectionUrlRef);
        ErrorCheck.ctorNotNull(DbConnectionPool.class, Properties.class, propsRef);
        dbConnectionUrl = dbConnectionUrlRef;
        props = propsRef;

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
    public void migrate(String dbType)
    {
        Flyway flyway = new Flyway();
        flyway.setSchemas(DATABASE_SCHEMA_NAME);
        flyway.setDataSource(dataSource);
        flyway.setTable(SCHEMA_HISTORY_TABLE_NAME);

        // When migrations are added in branches they can be applied in different orders
        flyway.setOutOfOrder(true);

        // Pass the DB type to the migrations
        flyway.setPlaceholders(ImmutableMap.of(LinstorMigration.PLACEHOLDER_KEY_DB_TYPE, dbType));

        flyway.setLocations(LinstorMigration.class.getPackage().getName());
        flyway.migrate();
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
            ConnectionFactory connFactory = new DriverManagerConnectionFactory(dbConnectionUrl, props);
            PoolableConnectionFactory poolConnFactory = new PoolableConnectionFactory(connFactory, null);

            GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
            poolConfig.setMinIdle(minIdleConnections);
            poolConfig.setMaxIdle(maxIdleConnections);
            poolConfig.setBlockWhenExhausted(true);
            poolConfig.setFairness(true);

            GenericObjectPool<PoolableConnection> connPool = new GenericObjectPool<>(poolConnFactory, poolConfig);

            poolConnFactory.setPool(connPool);
            poolConnFactory.setValidationQueryTimeout(dbTimeout);
            poolConnFactory.setMaxOpenPreparedStatements(dbMaxOpen);

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
}
