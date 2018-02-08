package com.linbit.linstor.dbcp;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.ControllerDatabase;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
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
public class DbConnectionPool implements ControllerDatabase
{
    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "SQL database connection pool service";

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
    private boolean started = false;

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
    public void initializeDataSource(String dbConnectionUrl, Properties props)
        throws SQLException
    {
        ErrorCheck.ctorNotNull(DbConnectionPool.class, String.class, dbConnectionUrl);
        ErrorCheck.ctorNotNull(DbConnectionPool.class, Properties.class, props);
        this.dbConnectionUrl = dbConnectionUrl;
        this.props = props;

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
        }
        return dbConn;
    }

    public void returnConnection(TransactionMgr transMgr)
    {
        if (transMgr != null)
        {
            returnConnection(transMgr.dbCon);
            transMgr.clearTransactionObjects();
        }
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

    /**
     * Closes all db connections the calling thread had not closed yet.
     *
     * @param skipConnections These connections will not be closed. Mainly used for cleaning up after tests
     * @return True if there was at least one open connection, false otherwise.
     */
    public boolean closeAllThreadLocalConnections(Connection... skipConnections)
    {
        List<Connection> skipConnList = Arrays.asList(skipConnections);
        boolean ret = false;
        List<Connection> list = threadLocalConnections.get();
        if (list != null)
        {
            for (Connection conn : list)
            {
                if (!skipConnList.contains(conn))
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
            started = false;
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
        poolConnFactory.setMaxOpenPrepatedStatements(dbMaxOpen);

        dataSource = new PoolingDataSource<PoolableConnection>(connPool);
        started = true;
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
        return started;
    }
}
