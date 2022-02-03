package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackSpec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

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
            MixedOperation<LinstorCrd<LinstorSpec>, KubernetesResourceList<LinstorCrd<LinstorSpec>>, Resource<LinstorCrd<LinstorSpec>>> client = currentTransactionRef
                .getClient(dbTable);

            for (Entry<String, LinstorCrd<?>> rsc : entry.getValue().entrySet())
            {
                numberOfUpdates++;
                LinstorCrd<?> updatedRsc = rsc.getValue();

                LinstorCrd<?> resource = client.withName(updatedRsc.getK8sKey()).get();
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
            MixedOperation<LinstorCrd<LinstorSpec>, KubernetesResourceList<LinstorCrd<LinstorSpec>>, Resource<LinstorCrd<LinstorSpec>>> client = currentTransactionRef
                .getClient(dbTable);

            for (Entry<String, LinstorCrd<?>> rsc : entry.getValue().entrySet())
            {
                numberOfUpdates++;
                LinstorCrd<?> deletedRsc = rsc.getValue();

                LinstorCrd<?> resource = client.withName(deletedRsc.getK8sKey()).get();
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
            currentTransactionRef.getRollbackClient().create(rollbackCrd);
        }
    }

    public static void cleanup(K8sCrdTransaction currentTransactionRef)
    {
        currentTransactionRef.getRollbackClient().delete();
    }

    public static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> void rollbackIfNeeded(
        K8sCrdTransaction currentTransactionRef,
        Function<SPEC, CRD> specToCrdRef
    )
    {
        List<RollbackCrd> rollbackList = currentTransactionRef.getRollbackClient().list().getItems();
        final int listSize = rollbackList.size();
        if (listSize == 1)
        {
            RollbackSpec rollbackSpec = rollbackList.get(0).getSpec();

            for (Entry<String, HashSet<String>> entry : rollbackSpec.getDeleteMap().entrySet())
            {
                DatabaseTable dbTable = GeneratedDatabaseTables.getByValue(entry.getKey());
                HashSet<String> keysToDelete = entry.getValue();
                delete(currentTransactionRef, dbTable, keysToDelete, specToCrdRef);
            }

            for (Entry<String, ? extends HashMap<String, GenericKubernetesResource>> entry : rollbackSpec
                .getRollbackMap().entrySet())
            {
                DatabaseTable dbTable = GeneratedDatabaseTables.getByValue(entry.getKey());
                ArrayList<GenericKubernetesResource> gkrList = new ArrayList<>();
                for (GenericKubernetesResource gkr : entry.getValue().values())
                {
                    gkrList.add(gkr);
                }

                restoreData(
                    currentTransactionRef,
                    dbTable,
                    gkrList,
                    specToCrdRef
                );
            }

            cleanup(currentTransactionRef);
        }
        else
        if (listSize > 1)
        {
            throw new ImplementationError("Unexpected count of rollback objects: " + rollbackList.size());
        }
        // else empty list, no-op
    }

    private static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> void delete(
        K8sCrdTransaction currentTransaction,
        DatabaseTable dbTable,
        HashSet<String> keysToDelete,
        Function<SPEC, CRD> specToCrdRef
    )
    {
        HashMap<String, SPEC> map = currentTransaction.getSpec(
            dbTable,
            spec -> keysToDelete.contains(spec.getK8sKey())
        );

        ArrayList<CRD> crdToDeleteList = new ArrayList<>();
        for (SPEC spec : map.values())
        {
            crdToDeleteList.add(specToCrdRef.apply(spec));
        }
        MixedOperation<CRD, KubernetesResourceList<CRD>, Resource<CRD>> client = currentTransaction
            .getClient(dbTable);
        client.delete(crdToDeleteList);
    }

    @SuppressWarnings("unchecked")
    private static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> void restoreData(
        K8sCrdTransaction currentTransaction,
        DatabaseTable dbTable,
        Collection<GenericKubernetesResource> valuesToRestore,
        Function<SPEC, CRD> specToCrdRef
    )
    {
        MixedOperation<LinstorCrd<SPEC>, KubernetesResourceList<LinstorCrd<SPEC>>, Resource<LinstorCrd<SPEC>>> client = currentTransaction
            .getClient(dbTable);
        Object typeEarasure = client;
        MixedOperation<GenericKubernetesResource, KubernetesResourceList<GenericKubernetesResource>, Resource<GenericKubernetesResource>> gkrClient = (MixedOperation<GenericKubernetesResource, KubernetesResourceList<GenericKubernetesResource>, Resource<GenericKubernetesResource>>) typeEarasure;
        for (GenericKubernetesResource gkr : valuesToRestore)
        {
            gkrClient.createOrReplace(gkr);
        }
    }
}
