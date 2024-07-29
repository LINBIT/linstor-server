package com.linbit.linstor.transaction;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.k8s.K8sResourceClient;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackSpec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;

public class ControllerK8sCrdRollbackMgr
{
    /**
     * Prepare (and store) data which we can roll back to if the next transaction fails to commit
     *
     * @param currentTransactionRef The current transaction.
     * @param maxRollbackEntries The maximum number of entries in a single rollback instance.
     */
    @SuppressWarnings("unchecked")
    public static void createRollbackEntry(K8sCrdTransaction currentTransactionRef, int maxRollbackEntries)
        throws DatabaseException
    {
        RollbackGenerator rollbackGen = new RollbackGenerator(maxRollbackEntries);

        for (Entry<DatabaseTable, HashMap<String, LinstorCrd<?>>> entry : currentTransactionRef.rscsToCreate.entrySet())
        {
            DatabaseTable dbTable = entry.getKey();
            for (String rscKey : entry.getValue().keySet())
            {
                rollbackGen.created(dbTable, rscKey);
            }
        }

        for (Entry<DatabaseTable, HashMap<String, LinstorCrd<?>>> entry : currentTransactionRef.rscsToReplace.entrySet())
        {
            DatabaseTable dbTable = entry.getKey();
            K8sResourceClient<LinstorCrd<LinstorSpec<?, ?>>> client;
            client = (K8sResourceClient<LinstorCrd<LinstorSpec<?, ?>>>) currentTransactionRef.getClient(dbTable);

            for (Entry<String, LinstorCrd<?>> rsc : entry.getValue().entrySet())
            {
                LinstorCrd<?> updatedRsc = rsc.getValue();

                LinstorCrd<?> resource = client.get(updatedRsc.getK8sKey());
                if (resource == null)
                {
                    throw new DatabaseException(
                        "Resource " + updatedRsc.getKind() + "/" + updatedRsc.getK8sKey() + " not found"
                    );
                }
                // Set the resource version so that we:
                // a) replace the thing we expect.
                // b) we can replace it in a single request.
                rsc.getValue().getMetadata().setResourceVersion(resource.getMetadata().getResourceVersion());
                rollbackGen.updatedOrDeleted(dbTable, resource);
            }
        }

        for (Entry<DatabaseTable, HashMap<String, LinstorCrd<?>>> entry : currentTransactionRef.rscsToDelete.entrySet())
        {
            DatabaseTable dbTable = entry.getKey();
            K8sResourceClient<LinstorCrd<LinstorSpec<?, ?>>> client;
            client = (K8sResourceClient<LinstorCrd<LinstorSpec<?, ?>>>) currentTransactionRef.getClient(dbTable);

            for (Entry<String, LinstorCrd<?>> rsc : entry.getValue().entrySet())
            {
                LinstorCrd<?> deletedRsc = rsc.getValue();

                LinstorCrd<?> resource = client.get(deletedRsc.getK8sKey());
                if (resource == null)
                {
                    throw new DatabaseException(
                        "Resource " + deletedRsc.getKind() + "/" + deletedRsc.getK8sKey() + " not found"
                    );
                }
                // Set the resource version so that we delete the thing we expect.
                rsc.getValue().getMetadata().setResourceVersion(resource.getMetadata().getResourceVersion());
                rollbackGen.updatedOrDeleted(dbTable, resource);
            }
        }

        // We only create a rollback resource if it is actually required, i.e. if we need to create/modify/delete more
        // than one resource in this transaction. Single object updates happen atomically, no need for rollback there.
        if (rollbackGen.getTotalUpdates() > 1)
        {
            List<RollbackCrd> rollbacks = rollbackGen.build();
            for (RollbackCrd crd : rollbacks)
            {
                // There might be an old rollback in some scenarios involving propsContainers
                // In that case our new rollback will also rollback all the old stuff, so it is
                // safe to replace
                currentTransactionRef.getRollbackClient().resource(crd).createOrReplace();
            }

            currentTransactionRef.setRollbacks(rollbacks);
        }
    }

    public static void cleanup(K8sCrdTransaction transactionToClean)
    {
        List<RollbackCrd> rollback = transactionToClean.getRollbacks();
        if (rollback != null)
        {
            // Delete all rollbacks
            transactionToClean.getRollbackClient().delete();
        }
    }

    public static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> void rollback(
        K8sCrdTransaction currentTransactionRef,
        Map<DatabaseTable, Supplier<K8sResourceClient<?>>> crdClientLutRef
    )
    {
        List<RollbackCrd> rollbacks = currentTransactionRef.getRollbacks();
        if (rollbacks != null)
        {
            Map<String, DatabaseTable> dbNamesToVersionedDatabaseTablesMap = new HashMap<>();
            for (DatabaseTable dbTable : crdClientLutRef.keySet())
            {
                dbNamesToVersionedDatabaseTablesMap.put(dbTable.getName(), dbTable);
            }

            for (RollbackCrd rollback : rollbacks)
            {
                for (Entry<String, HashSet<String>> entry : rollback.getSpec().getDeleteMap().entrySet())
                {
                    DatabaseTable dbTable = dbNamesToVersionedDatabaseTablesMap.get(entry.getKey());
                    HashSet<String> keysToDelete = entry.getValue();
                    delete(currentTransactionRef, dbTable, keysToDelete);
                }

                for (Entry<String, ? extends HashMap<String, GenericKubernetesResource>> entry : rollback.getSpec()
                    .getRollbackMap().entrySet())
                {
                    DatabaseTable dbTable = dbNamesToVersionedDatabaseTablesMap.get(entry.getKey());
                    ArrayList<GenericKubernetesResource> gkrList = new ArrayList<>(entry.getValue().values());

                    restoreData(
                        currentTransactionRef,
                        dbTable,
                        gkrList
                    );
                }
            }
        }

        cleanup(currentTransactionRef);
    }

    private static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> void delete(
        K8sCrdTransaction currentTransaction,
        DatabaseTable dbTable,
        HashSet<String> keysToDelete
    )
    {
        K8sResourceClient<CRD> client = currentTransaction
            .getClient(dbTable);
        for (String name : keysToDelete)
        {
            client.delete(name);
        }
    }

    @SuppressWarnings("unchecked")
    private static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> void restoreData(
        K8sCrdTransaction currentTransaction,
        DatabaseTable dbTable,
        Collection<GenericKubernetesResource> valuesToRestore
    )
    {
        K8sResourceClient<LinstorCrd<SPEC>> client = (K8sResourceClient<LinstorCrd<SPEC>>) currentTransaction
            .getClient(dbTable);
        Object typeErasure = client;
        K8sResourceClient<GenericKubernetesResource> gkrClient = (K8sResourceClient<GenericKubernetesResource>) typeErasure;
        for (GenericKubernetesResource gkr : valuesToRestore)
        {
            // Reset resource version, otherwise we incur extra requests while solving conflicts
            gkr.getMetadata().setResourceVersion("");
            gkr.getMetadata().setUid(null);
            gkrClient.createOrReplace(gkr);
        }
    }

    public static class RollbackGenerator
    {
        private int maxRollbackEntries;

        private @Nullable RollbackCrd currentRollback;
        private final List<RollbackCrd> rollbacks = new ArrayList<>();
        private int currentUpdates;

        private int totalUpdates;

        public RollbackGenerator(int maxRollbackEntriesRef)
        {
            maxRollbackEntries = maxRollbackEntriesRef;
        }

        public <SPEC extends LinstorSpec> void updatedOrDeleted(DatabaseTable table, LinstorCrd<SPEC> item)
        {
            ensureInvariants();
            currentUpdates++;
            totalUpdates++;
            currentRollback.getSpec().updatedOrDeleted(table, item);
        }

        public void created(DatabaseTable table, String key)
        {
            ensureInvariants();
            currentUpdates++;
            totalUpdates++;
            currentRollback.getSpec().created(table, key);
        }

        private void ensureInvariants()
        {
            if (currentUpdates == maxRollbackEntries)
            {
                rollbacks.add(currentRollback);
                currentUpdates = 0;
                currentRollback = null;
            }

            if (currentRollback == null)
            {
                currentRollback = new RollbackCrd(String.format("rollback-%d", rollbacks.size()), new RollbackSpec());
            }
        }

        public List<RollbackCrd> build()
        {
            if (currentUpdates > 0)
            {
                rollbacks.add(currentRollback);
                currentUpdates = 0;
                currentRollback = new RollbackCrd(String.format("rollback-%d", rollbacks.size()), new RollbackSpec());
            }

            return rollbacks;
        }

        public int getTotalUpdates()
        {
            return totalUpdates;
        }
    }
}
