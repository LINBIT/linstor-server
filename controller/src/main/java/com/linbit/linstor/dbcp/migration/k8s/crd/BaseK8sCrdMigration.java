package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.ImplementationError;
import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbcp.k8s.crd.DbK8sCrd;
import com.linbit.linstor.dbcp.migration.AbsMigration;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackCrd;
import com.linbit.linstor.transaction.ControllerK8sCrdTransactionMgr;
import com.linbit.linstor.transaction.K8sCrdMigrationContext;
import com.linbit.linstor.transaction.K8sCrdTransaction;
import com.linbit.timer.Delay;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

public abstract class BaseK8sCrdMigration extends AbsMigration
{
    protected final int version;
    protected final String description;
    protected final @Nullable K8sCrdMigrationContext fromCtx;
    protected final K8sCrdMigrationContext toCtx;

    private KubernetesClient k8sClient;

    protected K8sCrdTransaction txFrom;
    protected K8sCrdTransaction txTo;

    protected boolean schemeUpgraded = false;

    protected BaseK8sCrdMigration(K8sCrdMigrationContext fromToCtxRef)
    {
        this(fromToCtxRef, fromToCtxRef);
        schemeUpgraded = true; // nothing changed
    }

    protected BaseK8sCrdMigration(
        @Nullable K8sCrdMigrationContext fromCtxRef,
        K8sCrdMigrationContext toCtxRef
    )
    {
        K8sCrdMigration k8sMigAnnot = this.getClass().getAnnotation(K8sCrdMigration.class);
        version = k8sMigAnnot.version();
        description = k8sMigAnnot.description();

        toCtx = Objects.requireNonNull(toCtxRef);

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
            fromCtx = Objects.requireNonNull(fromCtxRef);
        }
        else
        {
            fromCtx = null;
        }
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

    protected void updateCrdSchemaForAllTables()
        throws DatabaseException
    {
        ensureRollbackCrdApplied();

        Function<DatabaseTable, String> dbTableToYamlLocation = toCtx.schemaCtx.getGetYamlLocations();

        HashSet<DatabaseTable> tablesToUpdate = new HashSet<>(
            Arrays.asList(toCtx.txMgrContext.getAllDatabaseTables())
        );
        for (DatabaseTable dbTable : tablesToUpdate)
        {
            String yamlLocation = dbTableToYamlLocation.apply(dbTable);
            if (yamlLocation != null)
            {
                /*
                 * otherwise GeneratedDatabaseTables.ALL_TABLES contains a table that the current version of the db /
                 * migration simply does not know. It should be save to ignore this case, since the migration will most
                 * likely not try to access a table it does not know?
                 */
                createOrReplaceCrdSchema(k8sClient, yamlLocation);
            }
        }

        Function<DatabaseTable, Class<? extends LinstorCrd<? extends LinstorSpec<?, ?>>>> tableMap;
        tableMap = toCtx.txMgrContext.getDbTableToCrdClass();

        long maxWaitUntil = System.currentTimeMillis() + 30 * 1_000; // 30 seconds
        long sleep = maxWaitUntil - System.currentTimeMillis();
        while (!tablesToUpdate.isEmpty() && sleep > 0)
        {
            ArrayList<DatabaseTable> updatedTables = new ArrayList<>();
            try
            {
                for (DatabaseTable table: tablesToUpdate)
                {
                    Class<? extends LinstorCrd<? extends LinstorSpec<?, ?>>> clazz = tableMap.apply(table);
                    if (clazz != null)
                    {
                        /*
                         * otherwise GeneratedDatabaseTables.ALL_TABLES contains a table that the current version of the
                         * db / migration simply does not know. It should be save to ignore this case, since the
                         * migration will most likely not try to access a table it does not know?
                         */
                        k8sClient.resources(clazz).list();
                    }
                    updatedTables.add(table);
                }
            }
            catch (KubernetesClientException exc)
            {
                if (exc.getCode() != 404)
                {
                    throw new DatabaseException("Failed to wait for updated CRDs", exc);
                }
            }

            tablesToUpdate.removeAll(updatedTables);

            sleep = maxWaitUntil - System.currentTimeMillis();
            Delay.sleep(2_000);
        }
        if (!tablesToUpdate.isEmpty())
        {
            throw new DatabaseException("Failed to update CRDs: " + tablesToUpdate);
        }

        if (fromCtx != null)
        {
            // if fromCtx is null, there cannot be db-tables to delete
            HashMap<String, DatabaseTable> dbTablesToDelete = new HashMap<>();
            for (DatabaseTable dbTable : fromCtx.txMgrContext.getAllDatabaseTables())
            {
                dbTablesToDelete.put(dbTable.getName(), dbTable);
            }
            for (DatabaseTable dbTable : toCtx.txMgrContext.getAllDatabaseTables())
            {
                dbTablesToDelete.remove(dbTable.getName());
            }
            Function<DatabaseTable, String> dbTableFromYamlLocation = fromCtx.schemaCtx.getGetYamlLocations();
            for (DatabaseTable dbTable : dbTablesToDelete.values())
            {
                String yamlLocation = dbTableFromYamlLocation.apply(dbTable);
                if (yamlLocation != null)
                {
                    deleteCrdSchema(k8sClient, yamlLocation);
                }
            }
        }

        schemeUpgraded = true;
    }

    protected void ensureRollbackCrdApplied() throws DatabaseException
    {
        createOrReplaceCrdSchema(k8sClient, "/com/linbit/linstor/dbcp/k8s/crd/Rollback.yaml");

        long maxWaitUntil = System.currentTimeMillis() + 30 * 1_000; // 30 seconds
        long sleep = maxWaitUntil - System.currentTimeMillis();
        boolean isRollbackReady = false;
        while (!isRollbackReady && sleep > 0)
        {
            try
            {
                k8sClient.resources(RollbackCrd.class).list();
                isRollbackReady = true;
            }
            catch (KubernetesClientException exc)
            {
                if (exc.getCode() != 404)
                {
                    throw new DatabaseException("Failed to wait for updated Rollback", exc);
                }
            }

            sleep = maxWaitUntil - System.currentTimeMillis();
            Delay.sleep(2_000);
        }

        if (!isRollbackReady)
        {
            throw new DatabaseException("Failed to wait for Rollback: not ready after timeout");
        }
    }

    protected void createOrReplaceCrdSchema(KubernetesClient k8s, String yamlLocation)
    {
        Resource<CustomResourceDefinition> crd = loadK8sCustomResource(k8s, yamlLocation);
        crd.forceConflicts().serverSideApply();
    }

    private void deleteCrdSchema(KubernetesClient k8s, String yamlLocation)
    {
        Resource<CustomResourceDefinition> crd = loadK8sCustomResource(k8s, yamlLocation);
        crd.delete();
    }

    @SuppressWarnings("checkstyle:LineLengthCheck")
    private Resource<CustomResourceDefinition> loadK8sCustomResource(
        KubernetesClient k8s,
        String yamlLocation
    )
    {
        NonNamespaceOperation<CustomResourceDefinition, CustomResourceDefinitionList, Resource<CustomResourceDefinition>> k8sApi = k8s
            .apiextensions()
            .v1()
            .customResourceDefinitions();

        return k8sApi.load(DbK8sCrd.class.getResourceAsStream(yamlLocation));
    }

    public void migrate(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        k8sClient = k8sDbRef.getClient();
        ControllerK8sCrdTransactionMgr txMgrTo = new ControllerK8sCrdTransactionMgr(
            k8sDbRef,
            toCtx.txMgrContext
        );

        txTo = txMgrTo.getTransaction();

        ControllerK8sCrdTransactionMgr txMgrFrom;
        if (fromCtx != null)
        {
            txMgrFrom = new ControllerK8sCrdTransactionMgr(
                k8sDbRef,
                fromCtx.txMgrContext
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

            if (!schemeUpgraded)
            {
                throw new ImplementationError(
                    "Migrating to new schema, but updateCrdSchemaForAllTables was not called!"
                );
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

        k8sDbRef.clearCache();
    }

    public abstract MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception;

    /**
     * <p>
     * Builds and returns a Map that contains all DatabaseTables as key from the "fromTx"'s ALL_TABLES and maps (if
     * available)
     * to the "toTx"'s ALL_TABLES instances. The reason for this is that when migrating from database version X to Y,
     * the loaded instances are returned with a reference to GenCrd${X}.GeneratedDatabaseTables whereas when we later
     * want to create the new versions we need to refer to GenCrd${Y}.GeneratedDatabaseTable's entries
     * </p>
     * <p>
     * If a DatabaseTable was newly introduced or deleted, the returned map will not contain an entry for it. In other
     * words, only entries that can bidirectionally be mapped are included in the returned map
     * </p>
     *
     * @return
     */
    protected HashMap<DatabaseTable, DatabaseTable> getDbTableRemapping()
    {
        HashMap<DatabaseTable, DatabaseTable> ret = new HashMap<>();
        // first, check if we indeed have different versions:
        if (Objects.equals(fromCtx.txMgrContext.getCrdVersion(), toCtx.txMgrContext.getCrdVersion()))
        {
            // same version, so just return a map filled with identities
            for (DatabaseTable fromDbTable : fromCtx.txMgrContext.getAllDatabaseTables())
            {
                ret.put(fromDbTable, fromDbTable);
            }
        }
        else
        {
            // different versions.
            HashMap<String, DatabaseTable> dbTblNameToTargetVersionDbTable = new HashMap<>();
            for (DatabaseTable dbTable : toCtx.txMgrContext.getAllDatabaseTables())
            {
                dbTblNameToTargetVersionDbTable.put(dbTable.getName(), dbTable);
            }

            for (DatabaseTable fromDbTable : fromCtx.txMgrContext.getAllDatabaseTables())
            {
                DatabaseTable newDbTable = dbTblNameToTargetVersionDbTable.get(fromDbTable.getName());
                if (newDbTable != null)
                {
                    ret.put(fromDbTable, newDbTable);
                }
            }
        }
        return ret;
    }

    protected static class MigrationResult
    {
        private boolean forceFromTxCommit = false;

        public void setForceFromTxCommit(boolean forceFromTxCommitRef)
        {
            forceFromTxCommit = forceFromTxCommitRef;
        }
    }
}
