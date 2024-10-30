package com.linbit.linstor.dbcp.etcd;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.core.ClassPathLoader;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbcp.DbUtils;
import com.linbit.linstor.dbcp.migration.etcd.BaseEtcdMigration;
import com.linbit.linstor.dbcp.migration.etcd.EtcdMigration;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    private static final String DB_HISTORY_VERSION_KEY_PRE34 = "LINSTOR/DBHISTORY/version";

    private final AtomicBoolean atomicStarted = new AtomicBoolean(false);

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
    public void preImportMigrateToVersion(String dbTypeRef, Object versionRef) throws DatabaseException
    {
        try
        {
            migrateToVersion(dbTypeRef, DbUtils.parseVersionAsInt(versionRef));
        }
        catch (InitializationException exc)
        {
            throw new DatabaseException(exc);
        }
    }

    @Override
    public void migrate(String dbType) throws InitializationException
    {
        migrateToVersion(dbType, MIGRATE_TO_MAX_VERSION);
    }

    private void migrateToVersion(String dbType, int targetVersionRef) throws InitializationException
    {
        ControllerETCDTransactionMgr etcdTxMgr = txMgrGenerator.startTransaction();
        etcdTxMgr.rollbackIfNeeded();
        EtcdTransaction etcdTx = etcdTxMgr.getTransaction();

        int dbVersion = getCurrentDbVersion(etcdTx);
        TreeMap<Integer, BaseEtcdMigration> migrations = buildMigrations();

        try
        {
            int highestKey;
            if (targetVersionRef == MIGRATE_TO_MAX_VERSION)
            {
                highestKey = migrations.lastKey();
            }
            else
            {
                highestKey = targetVersionRef;
            }

            while (dbVersion <= highestKey)
            {
                BaseEtcdMigration migration = migrations.get(dbVersion);
                if (migration == null)
                {
                    throw new ImplementationError(
                        "missing migration from dbVersion " +
                            (dbVersion - 1) + " -> " + dbVersion
                    );
                }
                errorReporter.logDebug("Migration DB: " + dbVersion + ": " + migration.getDescription());
                migration.migrate(etcdTx, EtcdUtils.linstorPrefix);

                dbVersion = migration.getNextVersion();
                etcdTx.put(EtcdUtils.linstorPrefix + "DBHISTORY/version", "" + dbVersion);

                etcdTxMgr.commit();
                etcdTx = etcdTxMgr.getTransaction();
            }
        }
        catch (Exception exc)
        {
            throw new LinStorDBRuntimeException("Exception occured during migration", exc);
        }
    }

    public int getCurrentVersion()
    {
        TreeMap<Integer, BaseEtcdMigration> migrations = buildMigrations();
        return migrations.lastKey();
    }

    @Override
    public boolean needsMigration(String dbType) throws InitializationException
    {
        ControllerETCDTransactionMgr etcdTxMgr = txMgrGenerator.startTransaction();
        etcdTxMgr.rollbackIfNeeded();

        EtcdTransaction etcdTx = etcdTxMgr.getTransaction();

        int dbVersion = getCurrentDbVersion(etcdTx);
        TreeMap<Integer, BaseEtcdMigration> migrations = buildMigrations();

        return dbVersion <= migrations.lastKey();
    }

    private int getCurrentDbVersion(EtcdTransaction tx) throws InitializationException
    {
        Map<String, String> dbHistoryVersionResponse = tx.get(EtcdUtils.linstorPrefix + "DBHISTORY/version");
        Map<String, String> dbHistoryVersionPre34 = tx.get(DB_HISTORY_VERSION_KEY_PRE34);

        int dbVersion = dbHistoryVersionResponse.size() > 0 ?
            Integer.parseInt(dbHistoryVersionResponse.values().iterator().next()) : 0;

        if ((dbVersion > 0 && dbVersion <= 34) || dbHistoryVersionPre34.size() > 0)
        {
            throw new InitializationException(
                "This Linstor version doesn't support upgrading from old etcd database. " +
                    "Last supported Linstor version for that is 1.8.x");
        }

        return dbVersion;
    }

    private TreeMap<Integer, BaseEtcdMigration> buildMigrations()
    {
        ClassPathLoader classPathLoader = new ClassPathLoader(errorReporter);
        List<Class<? extends BaseEtcdMigration>> etcdMigrationClasses = classPathLoader.loadClasses(
            BaseEtcdMigration.class.getPackage().getName(),
            Collections.singletonList(""),
            BaseEtcdMigration.class,
            EtcdMigration.class
        );

        TreeMap<Integer, BaseEtcdMigration> migrations = new TreeMap<>();
        try
        {
            for (Class<? extends BaseEtcdMigration> etcdMigrationClass : etcdMigrationClasses)
            {
                BaseEtcdMigration migration = etcdMigrationClass.newInstance();
                int version = migration.getVersion();
                if (migrations.containsKey(version))
                {
                    throw new ImplementationError(
                        "Duplicated migration version: " + version + ". " +
                            migrations.get(version).getDescription() + " " +
                            migration.getDescription()
                    );
                }
                migrations.put(version, migration);
            }

            checkIfAllMigrationsLinked(migrations);
        }
        catch (InstantiationException | IllegalAccessException exc)
        {
            throw new ImplementationError("Failed to load migrations for ETCD", exc);
        }

        return migrations;
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
            if (atomicStarted.compareAndSet(true, false))
            {
                etcdClient.close();
            }
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
        if (EtcdUtils.linstorPrefix == null)
        {
            throw new SystemServiceStartException("ETCD prefix is not set", true);
        }

        final String origConUrl = ctrlCfg.getDbConnectionUrl();
        final String connectionUrl = origConUrl.toLowerCase().startsWith(ETCD_SCHEME) ?
            origConUrl.substring(ETCD_SCHEME.length()) : origConUrl;

        EtcdClient.Builder builder = EtcdClient.forEndpoints(connectionUrl)
            .withMaxInboundMessageSize(Integer.MAX_VALUE);

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
                throw new SystemServiceStartException("Error configuring secure etcd connection", exc, true);
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
        atomicStarted.set(true);
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

        TreeMap<String, String> entries = etcdTx.get(EtcdUtils.linstorPrefix + "DBHISTORY/version", true);
        if (entries.size() == 0)
        {
            throw new DatabaseException("ETCD database reported 0 entries ");
        }
    }

    private void checkIfAllMigrationsLinked(TreeMap<Integer, BaseEtcdMigration> migrationsRef)
    {
        List<BaseEtcdMigration> unreachableMigrations = new ArrayList<>(migrationsRef.values());
        BaseEtcdMigration current = migrationsRef.get(0);
        while (current != null)
        {
            unreachableMigrations.remove(current);
            current = migrationsRef.get(current.getNextVersion());
        }
        if (!unreachableMigrations.isEmpty())
        {
            StringBuilder errorMsg = new StringBuilder("Found unreachable migrations: ");
            for (BaseEtcdMigration mig : unreachableMigrations)
            {
                errorMsg.append("  ").append(mig.getVersion()).append(" -> ").append(mig.getNextVersion())
                    .append(": ").append(mig.getClass().getSimpleName()).append(", ")
                    .append(mig.getDescription()).append("\n");
            }
            throw new ImplementationError(errorMsg.toString());
        }
    }
}
