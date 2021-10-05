package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbcp.k8s.crd.DbK8sCrd;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.transaction.K8sCrdSchemaUpdateContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionList;
import io.fabric8.kubernetes.client.KubernetesClient;
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
        K8sCrdSchemaUpdateContext rollbackYamlLocation
    )
        throws FileNotFoundException
    {
        Function<DatabaseTable, String> dbTableToYamlLocation = rollbackYamlLocation.getGetYamlLocations();
        for (DatabaseTable dbTable : GeneratedDatabaseTables.ALL_TABLES)
        {
            createOrReplaceCrdSchema(k8sClient, dbTableToYamlLocation.apply(dbTable));
        }
        // createOrReplaceCrdSchema(k8sClient, rollbackYamlLocation.getRollbackYamlLocation());
        createOrReplaceCrdSchema(k8sClient, "/com/linbit/linstor/dbcp/k8s/crd/Rollback.yaml");
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
