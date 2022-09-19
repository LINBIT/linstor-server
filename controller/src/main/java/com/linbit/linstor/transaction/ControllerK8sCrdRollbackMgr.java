package com.linbit.linstor.transaction;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.K8sResourceClient;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackSpec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;

public class ControllerK8sCrdRollbackMgr
{
    /**
     * Prepare (and store) data which we can rollback to if the next transaction fails to commit
     *
     * @param currentTransactionRef
     *
     * @return
     */
    public static void createRollbackEntry(K8sCrdTransaction currentTransactionRef)
        throws DatabaseException
    {
        RollbackSpec rollbackSpec = new RollbackSpec();
        RollbackCrd rollbackCrd = new RollbackCrd(rollbackSpec);
        int numberOfUpdates = 0;

        for (Entry<DatabaseTable, HashMap<String, LinstorCrd<?>>> entry : currentTransactionRef.rscsToCreate.entrySet())
        {
            DatabaseTable dbTable = entry.getKey();
            for (String rscKey : entry.getValue().keySet())
            {
                numberOfUpdates++;
                rollbackSpec.created(dbTable, rscKey);
            }
        }

        for (Entry<DatabaseTable, HashMap<String, LinstorCrd<?>>> entry : currentTransactionRef.rscsToReplace.entrySet())
        {
            DatabaseTable dbTable = entry.getKey();
            K8sResourceClient<LinstorCrd<LinstorSpec>> client = currentTransactionRef
                .getClient(dbTable);

            for (Entry<String, LinstorCrd<?>> rsc : entry.getValue().entrySet())
            {
                numberOfUpdates++;
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
                rollbackSpec.updatedOrDeleted(dbTable, resource);
            }
        }

        for (Entry<DatabaseTable, HashMap<String, LinstorCrd<?>>> entry : currentTransactionRef.rscsToDelete.entrySet())
        {
            DatabaseTable dbTable = entry.getKey();
            K8sResourceClient<LinstorCrd<LinstorSpec>> client = currentTransactionRef
                .getClient(dbTable);

            for (Entry<String, LinstorCrd<?>> rsc : entry.getValue().entrySet())
            {
                numberOfUpdates++;
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
                rollbackSpec.updatedOrDeleted(dbTable, resource);
            }
        }

        // We only create a rollback resource if it is actually required, i.e. if we need to create/modify/delete more
        // than one resource in this transaction. Single object updates happen atomically, no need for rollback there.
        if (numberOfUpdates > 1)
        {
            RollbackCrd oldRollback = currentTransactionRef.getRollback();
            if (oldRollback != null)
            {
                // There might be an old rollback in some scenarios involving propsContainers
                // In that case our new rollback will also rollback all the old stuff, so it is
                // safe to replace
                rollbackCrd.getMetadata().setResourceVersion(oldRollback.getMetadata().getResourceVersion());
                RollbackCrd applied = currentTransactionRef.getRollbackClient().replace(rollbackCrd);
                currentTransactionRef.setRollback(applied);
            }
            else
            {
                RollbackCrd applied = currentTransactionRef.getRollbackClient().create(rollbackCrd);
                currentTransactionRef.setRollback(applied);
            }
        }
    }

    public static void cleanup(K8sCrdTransaction transactionToClean)
    {
        RollbackCrd rollback = transactionToClean.getRollback();
        if (rollback != null)
        {
            transactionToClean.getRollbackClient().delete(rollback);
        }
    }

    public static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> void rollback(
        K8sCrdTransaction currentTransactionRef
    )
    {
        RollbackCrd rollback = currentTransactionRef.getRollback();
        if (rollback != null)
        {
            for (Entry<String, HashSet<String>> entry : rollback.getSpec().getDeleteMap().entrySet())
            {
                DatabaseTable dbTable = GeneratedDatabaseTables.getByValue(entry.getKey());
                HashSet<String> keysToDelete = entry.getValue();
                delete(currentTransactionRef, dbTable, keysToDelete);
            }

            for (Entry<String, ? extends HashMap<String, GenericKubernetesResource>> entry : rollback.getSpec()
                .getRollbackMap().entrySet())
            {
                DatabaseTable dbTable = GeneratedDatabaseTables.getByValue(entry.getKey());
                ArrayList<GenericKubernetesResource> gkrList = new ArrayList<>(entry.getValue().values());

                restoreData(
                    currentTransactionRef,
                    dbTable,
                    gkrList
                );
            }
        }

        cleanup(currentTransactionRef);
    }

    private static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> void delete(
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
    private static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> void restoreData(
        K8sCrdTransaction currentTransaction,
        DatabaseTable dbTable,
        Collection<GenericKubernetesResource> valuesToRestore
    )
    {
        K8sResourceClient<LinstorCrd<SPEC>> client = currentTransaction
            .getClient(dbTable);
        Object typeErasure = client;
        K8sResourceClient<GenericKubernetesResource> gkrClient = (K8sResourceClient<GenericKubernetesResource>) typeErasure;
        for (GenericKubernetesResource gkr : valuesToRestore)
        {
            // Reset resource version, otherwise we incur extra requests while solving conflicts
            gkr.getMetadata().setResourceVersion("");
            gkrClient.createOrReplace(gkr);
        }
    }
}
