package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.ImplementationError;
import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbcp.k8s.crd.DbK8sCrd;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackCrd;
import com.linbit.linstor.transaction.BaseControllerK8sCrdTransactionMgrContext;
import com.linbit.linstor.transaction.ControllerK8sCrdTransactionMgr;
import com.linbit.linstor.transaction.K8sCrdSchemaUpdateContext;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionCondition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionList;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

public abstract class BaseK8sCrdMigration
{
    protected final int version;
    protected final String description;
    protected final BaseControllerK8sCrdTransactionMgrContext upgradeToTxMgrContext;
    protected final BaseControllerK8sCrdTransactionMgrContext upgradeFromTxMgrContext;
    protected final K8sCrdSchemaUpdateContext upgradeToSchemaUpdateCtx;

    private KubernetesClient k8sClient;

    protected K8sCrdTransaction txFrom;
    protected K8sCrdTransaction txTo;

    public BaseK8sCrdMigration(
        BaseControllerK8sCrdTransactionMgrContext upgradeFromTxMgrContextRef,
        BaseControllerK8sCrdTransactionMgrContext upgradeToTxMgrContextRef,
        K8sCrdSchemaUpdateContext upgradeToSchemaUpdateCtxRef
    )
    {
        K8sCrdMigration k8sMigAnnot = this.getClass().getAnnotation(K8sCrdMigration.class);
        version = k8sMigAnnot.version();
        description = k8sMigAnnot.description();

        if (description.isEmpty())
        {
            throw new ImplementationError("Description must not be empty");
        }
        if (version == -1)
        {
            throw new ImplementationError("Version must not be -1");
        }

        if (version != 1)
        {
            upgradeFromTxMgrContext = Objects.requireNonNull(upgradeFromTxMgrContextRef);
        }
        else
        {
            upgradeFromTxMgrContext = upgradeFromTxMgrContextRef;
        }
        upgradeToTxMgrContext = Objects.requireNonNull(upgradeToTxMgrContextRef);
        upgradeToSchemaUpdateCtx = Objects.requireNonNull(upgradeToSchemaUpdateCtxRef);
    }

    public int getVersion()
    {
        return version;
    }

    public int getNextVersion()
    {
        return version + 1;
    }

    public String getDescription()
    {
        return description;
    }


    protected void updateCrdSchemaForAllTables() throws FileNotFoundException, DatabaseException
    {
        updateCrdSchemaForAllTables(upgradeToSchemaUpdateCtx);
    }

    protected void updateCrdSchemaForAllTables(K8sCrdSchemaUpdateContext upgradeToYamlFileLocationsRef)
        throws DatabaseException
    {
        K8sCrdSchemaUpdateContext updateCtx = upgradeToYamlFileLocationsRef;

        Function<DatabaseTable, String> dbTableToYamlLocation = updateCtx.getGetYamlLocations();
        Function<DatabaseTable, String> yamlKindNameFct = updateCtx.getGetYamlKindNameFunction();
        String targetVersion = updateCtx.getTargetVersion();

        HashSet<String> tablesToUpdate = new HashSet<>();
        for (DatabaseTable dbTable : GeneratedDatabaseTables.ALL_TABLES)
        {
            String tableToUpdate = yamlKindNameFct.apply(dbTable);
            if (tableToUpdate != null)
            {
                tablesToUpdate.add(tableToUpdate);
            }
        }
        tablesToUpdate.add("rollback");

        Watch watch = k8sClient.apiextensions().v1().customResourceDefinitions().watch(
            new Watcher<CustomResourceDefinition>()
            {
                @Override
                public void onClose(WatcherException causeRef)
                {
                    // ignored
                }

                @Override
                public void eventReceived(Action actionRef, CustomResourceDefinition resourceRef)
                {
                    String tableName = resourceRef.getStatus().getAcceptedNames().getKind().toLowerCase();
                    CustomResourceDefinitionStatus status = resourceRef.getStatus();
                    boolean containsTargetVersion;
                    if (tableName.equalsIgnoreCase("rollback"))
                    {
                        containsTargetVersion = true;
                    }
                    else
                    {
                        containsTargetVersion = status.getStoredVersions().contains(targetVersion);
                    }
                    if (status != null && status.getConditions() != null && containsTargetVersion)
                    {
                        for (CustomResourceDefinitionCondition condition : resourceRef.getStatus().getConditions())
                        {
                            if (condition.getType().equals("Established") &&
                                condition.getStatus().equalsIgnoreCase("true"))
                            {
                                synchronized (tablesToUpdate)
                                {
                                    tablesToUpdate.remove(tableName);
                                    tablesToUpdate.notify();
                                }
                            }
                        }
                    }
                }
            }
        );
        for (DatabaseTable dbTable : GeneratedDatabaseTables.ALL_TABLES)
        {
            createOrReplaceCrdSchema(k8sClient, dbTableToYamlLocation.apply(dbTable));
        }
        // createOrReplaceCrdSchema(k8sClient, rollbackYamlLocation.getRollbackYamlLocation());
        createOrReplaceCrdSchema(k8sClient, "/com/linbit/linstor/dbcp/k8s/crd/Rollback.yaml");

        synchronized (tablesToUpdate)
        {
            long maxWaitUntil = System.currentTimeMillis() + 30 * 1_000; // 30 seconds
            long sleep = maxWaitUntil - System.currentTimeMillis();
            while (!tablesToUpdate.isEmpty() && sleep > 0)
            {
                try
                {
                    tablesToUpdate.wait(sleep);
                }
                catch (InterruptedException ignored)
                {
                }
                sleep = maxWaitUntil - System.currentTimeMillis();
            }
        }
        watch.close();
        if (!tablesToUpdate.isEmpty())
        {
            throw new DatabaseException("Failed to update CRDs: " + tablesToUpdate);
        }
    }

    protected void createOrReplaceCrdSchema(KubernetesClient k8s, String yamlLocation)
    {
        NonNamespaceOperation<CustomResourceDefinition, CustomResourceDefinitionList, Resource<CustomResourceDefinition>> k8sApi = k8s
            .apiextensions().v1().customResourceDefinitions();

        CustomResourceDefinition crd = k8sApi
            .load(DbK8sCrd.class.getResourceAsStream(yamlLocation))
            .get();
        k8sApi.createOrReplace(crd);
    }

    public void migrate(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        k8sClient = k8sDbRef.getClient();
        ControllerK8sCrdTransactionMgr txMgrTo = new ControllerK8sCrdTransactionMgr(
            k8sDbRef,
            upgradeToTxMgrContext
        );
        txTo = txMgrTo.getTransaction();

        ControllerK8sCrdTransactionMgr txMgrFrom;
        if (upgradeFromTxMgrContext != null)
        {
            txMgrFrom = new ControllerK8sCrdTransactionMgr(
                k8sDbRef,
                upgradeFromTxMgrContext
            );
            txFrom = txMgrFrom.getTransaction();
        }
        else
        {
            txMgrFrom = null;
        }

        try
        {
            if (txFrom != null)
            {
                List<RollbackCrd> rollbackList = txFrom.getRollbackClient().list().getItems();
                if (!rollbackList.isEmpty())
                {
                    throw new DatabaseException(
                        "Cannot perform Migration " + version + " while a rollback has to be done"
                    );
                }
            }
            MigrationResult result = migrateImpl(k8sDbRef);
            if (result == null)
            {
                result = new MigrationResult();
            }

            txMgrTo.commit();
            if (txMgrFrom != null && (txMgrFrom.isDirty() || result.forceFromTxCommit))
            {
                txMgrFrom.commit();
            }
        }
        catch (Exception exc)
        {
            txMgrTo.rollback();
            if (txMgrFrom != null)
            {
                txMgrFrom.rollback();
            }
            throw exc;
        }
    }

    public abstract MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception;

    protected static class MigrationResult
    {
        private boolean forceFromTxCommit = false;

        public void setForceFromTxCommit(boolean forceFromTxCommitRef)
        {
            forceFromTxCommit = forceFromTxCommitRef;
        }
    }
}
