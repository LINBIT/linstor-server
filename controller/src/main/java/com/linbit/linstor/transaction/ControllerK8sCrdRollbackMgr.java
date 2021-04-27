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
import java.util.Set;
import java.util.function.Function;

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
        boolean hasContent = false;

        for (Entry<DatabaseTable, HashMap<String, LinstorCrd<?>>> entry : currentTransactionRef.rscsToChangeOrCreate
            .entrySet())
        {
            DatabaseTable dbTable = entry.getKey();
            HashMap<String, LinstorCrd<?>> rscsToChangeOrCreate = entry.getValue();

            final Set<String> changedOrDeletedKeys = entry.getValue().keySet();
            HashMap<String, LinstorSpec> map = currentTransactionRef.get(
                dbTable,
                spec -> changedOrDeletedKeys.contains(spec.getKey())
            );

            for (Entry<String, LinstorCrd<?>> rscToChangeOrCreate : rscsToChangeOrCreate.entrySet())
            {
                String specKey = rscToChangeOrCreate.getKey();
                LinstorSpec spec = map.get(specKey);
                if (spec == null)
                {
                    // db does not know about this entry. this rsc needs to be created.
                    rollbackSpec.created(dbTable, specKey);
                    hasContent = true;
                }
                else
                {
                    rollbackSpec.updatedOrDeleted(dbTable, spec);
                    hasContent = true;
                }
            }
        }

        for (Entry<DatabaseTable, HashMap<String, LinstorCrd<?>>> entry : currentTransactionRef.rscsToDelete.entrySet())
        {
            DatabaseTable dbTable = entry.getKey();
            for (LinstorCrd<?> crd : entry.getValue().values())
            {
                rollbackSpec.updatedOrDeleted(dbTable, crd.getSpec());
                hasContent = true;
            }
        }

        // try
        // {
        // System.out.println(new ObjectMapper(new YAMLFactory()).writeValueAsString(rollbackCrd));
        // }
        // catch (JsonProcessingException exc)
        // {
        // exc.printStackTrace();
        // }
        if (hasContent)
        {
            currentTransactionRef.getRollbackClient().createOrReplace(rollbackCrd);
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

            for (Entry<String, HashMap<String, LinstorSpec>> entry : rollbackSpec.getRollbackMap().entrySet())
            {
                DatabaseTable dbTable = GeneratedDatabaseTables.getByValue(entry.getKey());
                ArrayList<SPEC> specList = new ArrayList<>();
                for (LinstorSpec linstorSpec : entry.getValue().values())
                {
                    specList.add((SPEC) linstorSpec);
                }

                restoreData(
                    currentTransactionRef,
                    dbTable,
                    specList,
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

    @SuppressWarnings("unchecked")
    private static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> void delete(
        K8sCrdTransaction currentTransaction,
        DatabaseTable dbTable,
        HashSet<String> keysToDelete,
        Function<SPEC, CRD> specToCrdRef
    )
    {
        HashMap<String, SPEC> map = currentTransaction.get(
            dbTable,
            spec -> keysToDelete.contains(spec.getKey())
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
        Collection<SPEC> valuesToRestore,
        Function<SPEC, CRD> specToCrdRef
    )
    {
        MixedOperation<LinstorCrd<SPEC>, KubernetesResourceList<LinstorCrd<SPEC>>, Resource<LinstorCrd<SPEC>>> client = currentTransaction
            .getClient(dbTable);
        for (SPEC linstorSpec : valuesToRestore)
        {
            client.createOrReplace(specToCrdRef.apply(linstorSpec));
        }
    }
}
