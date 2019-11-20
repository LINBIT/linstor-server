package com.linbit.linstor.dbcp.etcd;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.core.LinstorConfigToml;
import com.linbit.linstor.dbcp.migration.etcd.Migration_00_Init;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.io.Files;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.KvStoreClient;
import com.ibm.etcd.client.kv.KvClient;

import static com.ibm.etcd.client.KeyUtils.bs;

@Singleton
public class DbEtcd implements ControllerETCDDatabase
{
    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "ETCD database handler";
    private static final String ETCD_SCHEME = "etcd://";

    private AtomicBoolean atomicStarted = new AtomicBoolean(false);

    private final ErrorReporter errorReporter;
    private final LinstorConfigToml linstorConfigToml;

    private int dbTimeout = ControllerDatabase.DEFAULT_TIMEOUT;
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
        ErrorReporter errorReporterRef,
        LinstorConfigToml linstorConfigTomlRef
    )
    {
        errorReporter = errorReporterRef;
        linstorConfigToml = linstorConfigTomlRef;
    }

    @Override
    public void setTimeout(int timeout)
    {
        dbTimeout = timeout;
    }

    @Override
    public void initializeDataSource(String dbConnectionUrl)
    {
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
            errorReporter.reportError(exc);
        }
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceName)
    {

    }

    @Override
    public void start() throws SystemServiceStartException
    {
        final String origConUrl = linstorConfigToml.getDB().getConnectionUrl();
        final String connectionUrl = origConUrl.toLowerCase().startsWith(ETCD_SCHEME) ?
            origConUrl.substring(ETCD_SCHEME.length()) : origConUrl;

        EtcdClient.Builder builder = EtcdClient.forEndpoints(connectionUrl);

        if (linstorConfigToml.getDB().getCACertificate() != null)
        {
            try
            {
                if (linstorConfigToml.getDB().getCACertificate() != null)
                {
                    builder.withCaCert(
                        Files.asByteSource(new File(linstorConfigToml.getDB().getCACertificate()))
                    );
                }

                if (linstorConfigToml.getDB().getClientCertificate() != null &&
                    linstorConfigToml.getDB().getClientKeyPKCS8PEM() != null)
                {
                    builder.withTlsConfig(sslContextBuilder ->
                        sslContextBuilder
                            .keyManager(
                                new File(linstorConfigToml.getDB().getClientCertificate()),
                                new File(linstorConfigToml.getDB().getClientKeyPKCS8PEM()),
                                linstorConfigToml.getDB().getClientKeyPassword()
                            )
                    );
                }
            }
            catch (IOException exc)
            {
                throw new SystemServiceStartException("Error configuring secure etcd connection", exc);
            }
        }
        else
        {
            builder.withPlainText();
        }

        if (linstorConfigToml.getDB().getUser() != null)
        {
            builder.withCredentials(
                linstorConfigToml.getDB().getUser(),
                linstorConfigToml.getDB().getPassword()
            );
        }

        etcdClient = builder.build();
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

    @Override
    public void checkHealth() throws DatabaseException
    {
        RangeResponse sync = etcdClient.getKvClient().get(bs(EtcdUtils.LINSTOR_PREFIX)).asPrefix().limit(1).sync();
        if (sync.getKvsList().size() == 0)
        {
            throw new DatabaseException("ETCD database reported 0 entries ");
        }
    }
}
