package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbcp.k8s.crd.DbK8sCrd;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.transaction.K8sCrdSchemaUpdateContext;

import java.io.FileNotFoundException;
import java.util.HashSet;
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
    private final int version;
    private final String description;

    public BaseK8sCrdMigration()
    {
        K8sCrdMigration k8sMigAnnot = this.getClass().getAnnotation(K8sCrdMigration.class);
        version = k8sMigAnnot.version();
        description = k8sMigAnnot.description();
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

    protected void updateCrdSchemaForAllTables(
        KubernetesClient k8sClient,
        K8sCrdSchemaUpdateContext updateCtx
    )
        throws FileNotFoundException, DatabaseException
    {
        Function<DatabaseTable, String> dbTableToYamlLocation = updateCtx.getGetYamlLocations();
        Function<DatabaseTable, String> yamlKindNameFct = updateCtx.getGetYamlKindNameFunction();
        String targetVersion = updateCtx.getTargetVersion();

        HashSet<String> tablesToUpdate = new HashSet<>();
        for (DatabaseTable dbTable : GeneratedDatabaseTables.ALL_TABLES)
        {
            tablesToUpdate.add(yamlKindNameFct.apply(dbTable));
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
                catch (InterruptedException exc)
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

    protected void createOrReplaceCrdSchema(KubernetesClient k8s, String yamlLocation) throws FileNotFoundException
    {
        NonNamespaceOperation<CustomResourceDefinition, CustomResourceDefinitionList, Resource<CustomResourceDefinition>> k8sApi = k8s
            .apiextensions().v1().customResourceDefinitions();

        CustomResourceDefinition crd = k8sApi
            .load(DbK8sCrd.class.getResourceAsStream(yamlLocation))
            .get();
        k8sApi.createOrReplace(crd);
    }

    public abstract void migrate(ControllerK8sCrdDatabase k8sDbRef) throws Exception;
}
