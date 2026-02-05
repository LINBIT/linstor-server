package com.linbit.linstor.dbcp.k8s.crd;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.ClassPathLoader;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbcp.DbUtils;
import com.linbit.linstor.dbcp.migration.k8s.crd.BaseK8sCrdMigration;
import com.linbit.linstor.dbcp.migration.k8s.crd.K8sCrdMigration;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.k8s.K8sCachingClient;
import com.linbit.linstor.dbdrivers.k8s.K8sResourceClient;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorVersionCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackCrd;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.transaction.ControllerK8sCrdTransactionMgr;
import com.linbit.linstor.transaction.ControllerK8sCrdTransactionMgrGenerator;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.inject.Provider;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionList;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.ApiextensionsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.utils.KubernetesSerialization;

@Singleton
public class DbK8sCrd implements ControllerK8sCrdDatabase
{
    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "K8s CRD handler";
    private static final String K8S_SCHEME = "k8s";

    /**
     * <p>This map stores the database versions that the current version of LINSTOR can no longer migrate from
     * due to the migrations having been consolidated.</p>
     *
     * <p>The key of an entry is the db-version that can only be applied by the LINSTOR version of the given value.
     * For example {@code <10, "v1.33.2">} means that if the current db-version is 10, the current LINSTOR version
     * can NOT migrate. The last LINSTOR version that could migrate from db version 10 was LINSTOR v1.33.2.</p>
     *
     * <p>Note that "version 10" here means that Migration_10_* was not yet applied. Db-version here refers to what
     * you get as a response from calling {@code tx.getDbVersion()}, but that is the <b>next</b> version that needs
     * to be applied. So if {@code tx.getDbVersion()} returns 10 that means that the current DB only has Migration_9
     * applied, not Migration_10. Since the original Migration_10_v1_19_1_CleanupOrphanedObjects was consolidated into
     * Migration_10_v1_19_1_ConsolidatedInit and can no longer be applied stand-alone in the current version of
     * LINSTOR, we write in this map {@code <10, "v1.33.2">}, since v1.33.2 was the last version that still contained
     * the original Migration_10_v1_19_1_CleanupOrphanedObjects.</p>
     *
     * <p>If {@code tx.getDbVersion()} returns for example 11, and we still have Migration_11 (has {@code version = 11}
     * in its annotation), we are fine and can continue with the migration-chain.</p>
     */
    private static final TreeMap<Integer, String> MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION;

    static
    {
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION = new TreeMap<>();
        // @checkstyle-suppress:magicnumber
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put(10, "v1.33.2");
        // when extending this map, we might want to calculate a more detailed upgrade path so that we can
        // write the user something like "v1.33.2 -> v1.35.4 -> v1.40.3 -> current" instead of having the user
        // find out how to properly upgrade.

        // @checkstyle-cancel-suppress
    }

    private final AtomicBoolean atomicStarted = new AtomicBoolean(false);
    private final ControllerK8sCrdTransactionMgrGenerator k8sTxGenerator;

    private final ErrorReporter errorReporter;
    private final CtrlConfig ctrlCfg;
    private final Provider<ControllerK8sCrdDatabase> controllerDatabaseProvider;

    private @Nullable KubernetesClient k8sClient;

    private @Nullable HashMap<Class<? extends LinstorCrd<? extends LinstorSpec<?, ?>>>, K8sResourceClient<?>> k8sCachingClient;

    static
    {
        try
        {
            SERVICE_NAME = new ServiceName("K8sCrdDatabaseService");
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(invalidNameExc);
        }
    }

    @Inject
    public DbK8sCrd(
        ErrorReporter errorReporterRef,
        CtrlConfig ctrlCfgRef,
        ControllerK8sCrdTransactionMgrGenerator k8sTxGeneratorRef,
        Provider<ControllerK8sCrdDatabase> controllerDatabaseProviderRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlCfg = ctrlCfgRef;
        k8sTxGenerator = k8sTxGeneratorRef;
        controllerDatabaseProvider = controllerDatabaseProviderRef;
    }

    @Override
    public void setTimeout(int timeoutRef)
    {
        // ignored (for now)
    }

    @Override
    public void initializeDataSource(String dbConnectionUrlRef) throws DatabaseException
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
    public void preImportMigrateToVersion(String dbTypeRef, Object versionRef) throws DatabaseException
    {
        try
        {
            migrate(DbUtils.parseVersionAsInt(versionRef));
        }
        catch (InitializationException exc)
        {
            throw new DatabaseException(exc);
        }
    }

    @Override
    public void migrate(String dbTypeRef) throws InitializationException
    {
        migrate(MIGRATE_TO_MAX_VERSION);
    }

    private void migrate(int targetVersionRef) throws InitializationException
    {
        ControllerK8sCrdTransactionMgr currentTx = k8sTxGenerator.startTransaction();
        currentTx.rollbackIfNeeded();

        TreeMap<Integer, BaseK8sCrdMigration> migrations = buildMigrations();

        ControllerK8sCrdDatabase k8sDb = controllerDatabaseProvider.get();

        int dbVersion = dbVersion(currentTx);
        if (dbVersion == 0)
        {
            errorReporter.logTrace("No database found. Creating migration resources.");
            // closable isn't used, as it closes the complete client connection
            // and would render transction objects useless
            // we close the client on shutdown anyway
            ApiextensionsAPIGroupDSL apiextensions = k8sDb.getClient().apiextensions();
            {
                NonNamespaceOperation<CustomResourceDefinition, CustomResourceDefinitionList, Resource<CustomResourceDefinition>> crdApi;
                crdApi = apiextensions.v1().customResourceDefinitions();

                Resource<CustomResourceDefinition> crd = crdApi.load(
                    DbK8sCrd.class.getResourceAsStream("/" + LinstorVersionCrd.getYamlLocation()));
                crd.serverSideApply();

                Resource<CustomResourceDefinition> rollback = crdApi.load(
                    DbK8sCrd.class.getResourceAsStream("/" + RollbackCrd.getYamlLocation()));
                rollback.serverSideApply();
            }
        }
        else
        {
            // check if we have already consolidated the required migration
            @Nullable Entry<Integer, String> ceilingEntry = MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION
                .ceilingEntry(dbVersion);
            if (ceilingEntry != null)
            {
                throw new InitializationException(
                    String.format(
                        "Local DB version (%d) is too old. The current LINSTOR version can only migrate DB " +
                            "versions from and including %d. The last version of LINSTOR that supports migration of " +
                            "local DB is %s",
                        dbVersion,
                        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.lastKey() + 1,
                        ceilingEntry.getValue()
                    )
                );
            }
        }

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

                @Nullable BaseK8sCrdMigration targetMigration = migrations.get(highestKey);
                if (targetMigration == null)
                {
                    throw new InitializationException(
                        "Target migration version '" + targetVersionRef + "' does not exist"
                    );
                }
            }

            while (dbVersion <= highestKey)
            {
                BaseK8sCrdMigration migration = migrations.get(dbVersion);
                final int fromVersion;
                if (migration.isInitial())
                {
                    fromVersion = 0;
                }
                else
                {
                    fromVersion = migration.getVersion() - 1;
                }
                final int toVersion = migration.getNextVersion() - 1;
                errorReporter.logDebug(
                    "Migration DB: %d -> %d: %s",
                    fromVersion,
                    toVersion,
                    migration.getDescription()
                );
                migration.migrate(k8sDb);

                dbVersion = migration.getNextVersion();

                currentTx.getTransaction().updateLinstorVersion(dbVersion);

                currentTx.commit();
                currentTx = k8sTxGenerator.startTransaction();
            }
        }
        catch (Exception exc)
        {
            throw new LinStorDBRuntimeException("Exception occurred during migration", exc);
        }
    }

    public int getCurrentVersion()
    {
        TreeMap<Integer, BaseK8sCrdMigration> migrations = buildMigrations();
        return migrations.lastKey();
    }

    @Override
    public boolean needsMigration(String dbType)
    {
        ControllerK8sCrdTransactionMgr currentTx = k8sTxGenerator.startTransaction();
        TreeMap<Integer, BaseK8sCrdMigration> migrations = buildMigrations();
        int dbVersion = dbVersion(currentTx);

        return dbVersion <= migrations.lastKey();
    }

    private int dbVersion(ControllerK8sCrdTransactionMgr tx)
    {
        Integer dbVersion = tx.getDbVersion();

        if (dbVersion == null)
        {
            dbVersion = 0;
        }
        else
        {
            errorReporter.logTrace("Found database version %d", dbVersion - 1);
        }

        return dbVersion;
    }
    private TreeMap<Integer, BaseK8sCrdMigration> buildMigrations()
    {
        ClassPathLoader classPathLoader = new ClassPathLoader(errorReporter);
        List<Class<? extends BaseK8sCrdMigration>> k8sMigrationClasses = classPathLoader.loadClasses(
            BaseK8sCrdMigration.class.getPackage().getName(),
            Collections.singletonList(""),
            BaseK8sCrdMigration.class,
            K8sCrdMigration.class
        );

        TreeMap<Integer, BaseK8sCrdMigration> migrations = new TreeMap<>();
        try
        {
            @Nullable BaseK8sCrdMigration initMigration = null;
            for (Class<? extends BaseK8sCrdMigration> k8sMigrationClass : k8sMigrationClasses)
            {
                BaseK8sCrdMigration migration = k8sMigrationClass.getDeclaredConstructor().newInstance();
                int version = migration.getVersion();
                if (migrations.containsKey(version))
                {
                    throw new ImplementationError(
                        "Duplicated migration version: " + version + ". " +
                            migrations.get(version).getDescription() + " " +
                            migration.getDescription()
                    );
                }
                if (migration.isInitial())
                {
                    if (initMigration != null)
                    {
                        throw new ImplementationError(
                            "Multiple initial migrations found!\n" + migration.getDescription() + "\n" +
                                initMigration.getDescription()
                        );
                    }
                    initMigration = migration;
                }
                migrations.put(version, migration);
            }
            if (initMigration == null)
            {
                throw new ImplementationError("No initial migration found!");
            }
            if (!initMigration.equals(migrations.firstEntry().getValue()))
            {
                throw new ImplementationError(
                    "Initial migration is not the oldest migration!\nInitial: " + initMigration.getDescription() +
                        "\nOldest: " + migrations.firstEntry().getValue()
                );
            }
            // set the initial migration to version 0, so we can start the loop at 0
            migrations.put(0, initMigration);

            checkIfAllMigrationsLinked(migrations);
        }
        catch (ReflectiveOperationException exc)
        {
            throw new ImplementationError("Failed to migrate", exc);
        }

        return migrations;
    }

    private void checkIfAllMigrationsLinked(TreeMap<Integer, BaseK8sCrdMigration> migrationsRef)
    {
        Set<BaseK8sCrdMigration> unreachableMigrations = new HashSet<>(migrationsRef.values());
        BaseK8sCrdMigration current = migrationsRef.get(0);
        while (current != null)
        {
            unreachableMigrations.remove(current);
            current = migrationsRef.get(current.getNextVersion());
        }
        if (!unreachableMigrations.isEmpty())
        {
            StringBuilder errorMsg = new StringBuilder("Found unreachable migrations: ");
            for (BaseK8sCrdMigration mig : unreachableMigrations)
            {
                errorMsg.append("  ").append(mig.getVersion()).append(" -> ").append(mig.getNextVersion())
                    .append(": ").append(mig.getClass().getSimpleName()).append(", ")
                    .append(mig.getDescription()).append("\n");
            }
            throw new ImplementationError(errorMsg.toString());
        }
    }

    @Override
    public boolean closeAllThreadLocalConnections()
    {
        return true;
    }

    @Override
    public void shutdown(boolean ignoredJvmShutdownRef)
    {
        try
        {
            if (atomicStarted.compareAndSet(true, false))
            {
                k8sClient.close();
            }
        }
        catch (Exception exc)
        {
            errorReporter.reportError(exc);
        }
    }

    @Override
    public void checkHealth() throws DatabaseException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setServiceInstanceName(ServiceName instanceNameRef)
    {

    }

    @Override
    public void start() throws SystemServiceStartException
    {
        if (ctrlCfg.getDbConnectionUrl() == null || !K8S_SCHEME.equalsIgnoreCase(ctrlCfg.getDbConnectionUrl().trim()))
        {
            throw new SystemServiceStartException("Not configured for kubernetes connection!", true);
        }

        final ObjectMapper customMapper = new ObjectMapper();
        k8sClient = new KubernetesClientBuilder()
            .withConfig(new ConfigBuilder().withRequestRetryBackoffLimit(ctrlCfg.getK8sRequestRetries()).build())
            .withKubernetesSerialization(new KubernetesSerialization(customMapper, true))
            .build();
        // farbric8 KubernetesClient library changed the default serialization behavior with version 6.13.0
        // as we rely on the old behavior we revert this, to the old write DATE as timestamps
        // https://github.com/fabric8io/kubernetes-client/pull/5962
        customMapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        k8sCachingClient = new HashMap<>();
        atomicStarted.set(true);
    }

    @Override
    public void awaitShutdown(long timeoutRef) throws InterruptedException
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
    public KubernetesClient getClient()
    {
        return k8sClient;
    }

    @Override
    public K8sResourceClient<?> getCachingClient(Class<? extends LinstorCrd<? extends LinstorSpec<?, ?>>> clazz)
    {
        return k8sCachingClient.computeIfAbsent(clazz, c -> new K8sCachingClient<>(k8sClient.resources(c)));
    }

    @Override
    public int getMaxRollbackEntries()
    {
        return ctrlCfg.getK8sMaxRollbackEntries();
    }

    @Override
    public void clearCache()
    {
        k8sCachingClient.clear();
    }
}
