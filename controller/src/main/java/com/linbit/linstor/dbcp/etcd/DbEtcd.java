package com.linbit.linstor.dbcp.etcd;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.dbcp.migration.etcd.Migration_00_Init;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.KvStoreClient;
import com.ibm.etcd.client.kv.KvClient;

import static com.ibm.etcd.client.KeyUtils.bs;


public class DbEtcd implements ControllerETCDDatabase
{
    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "ETCD database handler";
    public static final String LINSTOR_PREFIX = "LINSTOR/";

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
        String dbhistoryVersionKey = LINSTOR_PREFIX + "DBHISTORY/version";

        RangeResponse dbVersResp = etcdClient.getKvClient().get(bs(dbhistoryVersionKey)).sync();
        int dbVersion = dbVersResp.getCount() > 0 ?
            Integer.parseInt(dbVersResp.getKvs(0).getValue().toStringUtf8()) : 0;

        if (dbVersion == 0)
        {
            Migration_00_Init.migrate(etcdClient.getKvClient());
        }
    }

    public static Map<String, String> getTableRow(KvClient client, String key)
    {
        RangeResponse rspRow = client.get(bs(key)).asPrefix().sync();

        HashMap<String, String> rowMap = new HashMap<>();
        for (KeyValue keyValue : rspRow.getKvsList())
        {
            final String recKey = keyValue.getKey().toStringUtf8();
            final String columnName = recKey.substring(recKey.lastIndexOf("/") + 1);
            rowMap.put(columnName, keyValue.getValue().toStringUtf8());
        }

        return rowMap;
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
