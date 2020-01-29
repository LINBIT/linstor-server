package com.linbit.linstor.dbcp.etcd;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbcp.migration.etcd.Migration_00_Init;
import com.linbit.linstor.dbcp.migration.etcd.Migration_01_DelEmptyRscExtNames;
import com.linbit.linstor.dbcp.migration.etcd.Migration_02_AutoQuorumAndTiebreaker;
import com.linbit.linstor.dbcp.migration.etcd.Migration_03_DelProp_SnapshotRestore;
import com.linbit.linstor.dbcp.migration.etcd.Migration_04_DisklessFlagSplit;
import com.linbit.linstor.dbcp.migration.etcd.Migration_05_UnifyResourcesAndSnapshots;
import com.linbit.linstor.dbcp.migration.etcd.Migration_06_RscGrpDfltReplCount;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.transaction.ControllerETCDTransactionMgr;
import com.linbit.linstor.transaction.ControllerETCDTransactionMgrGenerator;
import com.linbit.linstor.transaction.EtcdTransaction;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.io.Files;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.KvStoreClient;
import com.ibm.etcd.client.kv.KvClient;

@Singleton
public class DbEtcd implements ControllerETCDDatabase
{
    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "ETCD database handler";
    private static final String ETCD_SCHEME = "etcd://";
    private static final String DB_HISTORY_VERSION_KEY = EtcdUtils.LINSTOR_PREFIX + "DBHISTORY/version";

    private AtomicBoolean atomicStarted = new AtomicBoolean(false);

    private final ErrorReporter errorReporter;
    private final CtrlConfig ctrlCfg;
    private final ControllerETCDTransactionMgrGenerator txMgrGenerator;

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
        CtrlConfig ctrlCfgRef,
        ControllerETCDTransactionMgrGenerator txMgrGenRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlCfg = ctrlCfgRef;
        txMgrGenerator = txMgrGenRef;
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
        ControllerETCDTransactionMgr etcdTxMgr = txMgrGenerator.startTransaction();
        EtcdTransaction etcdTx = etcdTxMgr.getTransaction();

        Map<String, String> dbHistoryVersionResponse = etcdTx.get(DB_HISTORY_VERSION_KEY);

        int dbVersion = dbHistoryVersionResponse.size() > 0
            ? Integer.parseInt(dbHistoryVersionResponse.values().iterator().next())
            : 0;

        if (dbVersion == 0)
        {
            Migration_00_Init.migrate(etcdTx);
            dbVersion++;
            etcdTxMgr.commit();
        }
        TreeMap<Integer, EtcdMigrationMethod> migrations = new TreeMap<>();
        migrations.put(0, Migration_00_Init::migrate);
        migrations.put(1, Migration_01_DelEmptyRscExtNames::migrate);
        migrations.put(2, Migration_02_AutoQuorumAndTiebreaker::migrate);
        migrations.put(3, Migration_03_DelProp_SnapshotRestore::migrate);
        // we introduced a bug where instead of writing (3+1) we accidentally written
        // ("3" + "1").
        migrations.put(31, Migration_04_DisklessFlagSplit::migrate);
        migrations.put(32, Migration_05_UnifyResourcesAndSnapshots::migrate);
        migrations.put(33, Migration_06_RscGrpDfltReplCount::migrate);

        try
        {
            int highestKey = migrations.lastKey();
            for (; dbVersion <= highestKey; dbVersion++)
            {
                if (dbVersion == 4)
                {
                    // we introduced a bug where instead of writing (3+1) we accidentally written
                    // ("3" + "1").
                    dbVersion = 31;
                }

                EtcdMigrationMethod migrationMethod = migrations.get(dbVersion);
                if (migrationMethod == null)
                {
                    throw new ImplementationError(
                        "missing migration from dbVersion " +
                            (dbVersion - 1) + " -> " + dbVersion
                    );
                }
                migrationMethod.migrate(etcdTx);

                etcdTx.put(EtcdUtils.LINSTOR_PREFIX + "DBHISTORY/version", "" + (dbVersion + 1));

                etcdTxMgr.commit();
            }
        }
        catch (Exception exc)
        {
            throw new LinStorDBRuntimeException("Exception occured during migration", exc);
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
        final String origConUrl = ctrlCfg.getDbConnectionUrl();
        final String connectionUrl = origConUrl.toLowerCase().startsWith(ETCD_SCHEME) ?
            origConUrl.substring(ETCD_SCHEME.length()) : origConUrl;

        EtcdClient.Builder builder = EtcdClient.forEndpoints(connectionUrl);

        if (ctrlCfg.getDbCaCertificate() != null)
        {
            try
            {
                if (ctrlCfg.getDbCaCertificate() != null)
                {
                    builder.withCaCert(
                        Files.asByteSource(new File(ctrlCfg.getDbCaCertificate()))
                    );
                }

                if (ctrlCfg.getDbClientCertificate() != null &&
                    ctrlCfg.getDbClientKeyPkcs8Pem() != null)
                {
                    builder.withTlsConfig(sslContextBuilder ->
                        sslContextBuilder
                            .keyManager(
                                new File(ctrlCfg.getDbClientCertificate()),
                                new File(ctrlCfg.getDbClientKeyPkcs8Pem()),
                                ctrlCfg.getDbClientKeyPassword()
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

        if (ctrlCfg.getDbUser() != null)
        {
            builder.withCredentials(
                ctrlCfg.getDbUser(),
                ctrlCfg.getDbPassword()
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
        ControllerETCDTransactionMgr etcdTxMgr = txMgrGenerator.startTransaction();
        EtcdTransaction etcdTx = etcdTxMgr.getTransaction();

        TreeMap<String, String> entries = etcdTx.get(DB_HISTORY_VERSION_KEY, true);
        if (entries.size() == 0)
        {
            throw new DatabaseException("ETCD database reported 0 entries ");
        }
    }

    private interface EtcdMigrationMethod
    {
        void migrate(EtcdTransaction tx) throws Exception;
    }
}
