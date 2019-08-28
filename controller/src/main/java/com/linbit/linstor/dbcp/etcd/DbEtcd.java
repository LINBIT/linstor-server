package com.linbit.linstor.dbcp.etcd;

import static com.ibm.etcd.client.KeyUtils.bs;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.dbcp.migration.etcd.Migration_00_Init;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.concurrent.atomic.AtomicBoolean;

import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.KvStoreClient;
import com.ibm.etcd.client.kv.KvClient;

@Singleton
public class DbEtcd implements ControllerETCDDatabase
{
    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "ETCD database handler";

    private AtomicBoolean atomicStarted = new AtomicBoolean(false);

    private int dbTimeout = ControllerDatabase.DEFAULT_TIMEOUT;
    private String connectionUrl;
    private KvStoreClient etcdClient;

    static
    {
        try
        {
            SERVICE_NAME = new ServiceName("ETCDDatabaseService");
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(invalidNameExc);
        }
    }

    @Inject
    public DbEtcd(
    )
    {
    }

    @Override
    public void setTimeout(int timeout)
    {
        dbTimeout = timeout;
    }

    @Override
    public void initializeDataSource(String dbConnectionUrl)
    {
        connectionUrl = dbConnectionUrl;
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
    public KvClient getKvClient()
    {
        return etcdClient.getKvClient();
    }

    @Override
    public void migrate(String dbType)
    {
        // do manual data migration and initial data
        String dbhistoryVersionKey = EtcdUtils.LINSTOR_PREFIX + "DBHISTORY/version";

        RangeResponse dbVersResp = etcdClient.getKvClient().get(bs(dbhistoryVersionKey)).sync();
        int dbVersion = dbVersResp.getCount() > 0 ?
            Integer.parseInt(dbVersResp.getKvs(0).getValue().toStringUtf8()) : 0;

        if (dbVersion == 0)
        {
            Migration_00_Init.migrate(etcdClient.getKvClient());
        }
    }

    @Override
    public boolean closeAllThreadLocalConnections()
    {
        return true;
    }

    @Override
    public void shutdown()
    {
        try
        {
            etcdClient.close();
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

    }

    @Override
    public void start() throws SystemServiceStartException
    {
        etcdClient = EtcdClient.forEndpoints(connectionUrl).withPlainText().build();
    }

    @Override
    public void awaitShutdown(long timeout) throws InterruptedException
    {

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
        return SERVICE_NAME;
    }

    @Override
    public boolean isStarted()
    {
        return atomicStarted.get();
    }
}
