package com.linbit.linstor.dbdrivers;

import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.ControllerDatabase;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.inject.Inject;

public class EmptyControllerDatabase implements ControllerDatabase
{
    @Inject
    public EmptyControllerDatabase()
    {
    }

    @Override
    public boolean isStarted()
    {
        return false;
    }

    @Override
    public ServiceName getServiceName()
    {
        return null;
    }

    @Override
    public String getServiceInfo()
    {
        return null;
    }

    @Override
    public ServiceName getInstanceName()
    {
        return null;
    }

    @Override
    public void start() throws SystemServiceStartException
    {
        // no-op
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceName)
    {
        // no-op
    }

    @Override
    public void awaitShutdown(long timeout) throws InterruptedException
    {
        // no-op
    }

    @Override
    public void shutdown()
    {
        // no-op
    }

    @Override
    public void setTimeout(int timeout)
    {
        // no-op
    }

    @Override
    public void setMaxOpenPreparedStatements(int maxOpen)
    {
        // no-op
    }

    @Override
    public void returnConnection(Connection dbConn)
    {
        // no-op
    }

    @Override
    public void initializeDataSource(String dbConnectionUrl, Properties props) throws SQLException
    {
        // no-op
    }

    @Override
    public Connection getConnection() throws SQLException
    {
        return null;
    }
}
