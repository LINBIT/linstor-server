package com.linbit.linstor.transaction;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorVersionCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackCrd;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

public class ControllerK8sCrdTransactionMgr implements TransactionMgrK8sCrd
{
    private final TransactionObjectCollection transactionObjectCollection;
    private final ControllerK8sCrdDatabase controllerK8sCrdDatabase;

    private K8sCrdTransaction currentTransaction;

    private final Map<DatabaseTable, MixedOperation<?, ?, ?>> crdClientLut;
    private final MixedOperation<RollbackCrd, KubernetesResourceList<RollbackCrd>, Resource<RollbackCrd>> rollbackClient;
    private final MixedOperation<LinstorVersionCrd, KubernetesResourceList<LinstorVersionCrd>, Resource<LinstorVersionCrd>> linstorVersionClient;
    private final Function<LinstorSpec, LinstorCrd<LinstorSpec>> specToCrd;
    private final KubernetesClient k8sClient;

    public ControllerK8sCrdTransactionMgr(ControllerK8sCrdDatabase controllerK8sCrdDatabaseRef)
    {
        this(
            controllerK8sCrdDatabaseRef,
            new BaseControllerK8sCrdTransactionMgrContext(
                GenCrdCurrent::databaseTableToCustomResourceClass,
                GenCrdCurrent::specToCrd
            )
        );
    }

    public ControllerK8sCrdTransactionMgr(
        ControllerK8sCrdDatabase controllerK8sCrdDatabaseRef,
        BaseControllerK8sCrdTransactionMgrContext ctx
    )
    {
        controllerK8sCrdDatabase = controllerK8sCrdDatabaseRef;
        specToCrd = ctx.getSpecToCrd();
        transactionObjectCollection = new TransactionObjectCollection();

        k8sClient = controllerK8sCrdDatabaseRef.getClient();

        crdClientLut = new HashMap<>();
        Function<DatabaseTable, Class<? extends LinstorCrd<? extends LinstorSpec>>> dbTableToCrdClass = ctx
            .getDbTableToCrdClass();
        for (DatabaseTable tbl : GeneratedDatabaseTables.ALL_TABLES)
        {
            crdClientLut.put(
                tbl,
                k8sClient.resources(dbTableToCrdClass.apply(tbl))
            );
        }
        rollbackClient = k8sClient.resources(RollbackCrd.class);
        linstorVersionClient = k8sClient.resources(LinstorVersionCrd.class);

        currentTransaction = createNewTx();
    }

    private K8sCrdTransaction createNewTx()
    {
        return new K8sCrdTransaction(crdClientLut, rollbackClient, linstorVersionClient);
    }

    public Integer getDbVersion()
    {
        Integer ret = null;
        CustomResourceDefinitionList list = k8sClient.apiextensions().v1().customResourceDefinitions()
            .list(new ListOptionsBuilder().build());
        if (!list.getItems().isEmpty())
        {
            for (CustomResourceDefinition crd : list.getItems())
            {
                if (crd.getStatus().getAcceptedNames().getPlural().equals(LinstorVersionCrd.LINSTOR_CRD_NAME))
                {
                    // k8s knows about the schema, we still need to get the actual linstorversion instance
                    List<LinstorVersionCrd> linstorVersionList = linstorVersionClient.list().getItems();
                    if (linstorVersionList != null && !linstorVersionList.isEmpty())
                    {
                        ret = linstorVersionList.get(0).getSpec().version;
                    }
                    break;
                }
            }
        }
        return ret;
    }

    @Override
    public K8sCrdTransaction getTransaction()
    {
        return currentTransaction;
    }

    @Override
    public void register(TransactionObject transObjRef)
    {
        transactionObjectCollection.register(transObjRef);
    }

    @Override
    public void commit() throws TransactionException
    {
        try
        {
            ControllerK8sCrdRollbackMgr.createRollbackEntry(currentTransaction);
        }
        catch (DatabaseException exc)
        {
            throw new TransactionException("Error creating rollback entry", exc);
        }

        for (Entry<DatabaseTable, HashMap<String, LinstorCrd<?>>> entry : currentTransaction.rscsToChangeOrCreate
            .entrySet())
        {
            createOrReplace(entry.getKey(), entry.getValue());
        }
        for (Entry<DatabaseTable, HashMap<String, LinstorCrd<?>>> entry : currentTransaction.rscsToDelete.entrySet())
        {
            delete(entry.getKey(), entry.getValue());
        }

        transactionObjectCollection.commitAll();

        clearTransactionObjects();

        currentTransaction = createNewTx();

        ControllerK8sCrdRollbackMgr.cleanup(currentTransaction);
    }

    @SuppressWarnings("unchecked")
    private <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> void createOrReplace(
        DatabaseTable dbTableRef,
        HashMap<String, LinstorCrd<?>> changedCrds
    )
    {
        MixedOperation<CRD, KubernetesResourceList<CRD>, Resource<CRD>> client = currentTransaction
            .getClient(dbTableRef);
        for (LinstorCrd<?> linstorCrd : changedCrds.values())
        {
            client.createOrReplace((CRD) linstorCrd);
        }
    }

    @SuppressWarnings("unchecked")
    private <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> void delete(
        DatabaseTable dbTableRef,
        HashMap<String, LinstorCrd<?>> createdCrds
    )
    {
        MixedOperation<CRD, KubernetesResourceList<CRD>, Resource<CRD>> client = currentTransaction
            .getClient(dbTableRef);
        for (LinstorCrd<?> linstorCrd : createdCrds.values())
        {
            client.delete((CRD) linstorCrd);
        }
    }

    @Override
    public void rollback() throws TransactionException
    {
        ControllerK8sCrdRollbackMgr.rollbackIfNeeded(currentTransaction, specToCrd);

        transactionObjectCollection.rollbackAll();

        currentTransaction = createNewTx();

        clearTransactionObjects();
    }

    @Override
    public void clearTransactionObjects()
    {
        transactionObjectCollection.clearAll();
    }

    @Override
    public boolean isDirty()
    {
        return transactionObjectCollection.areAnyDirty();
    }

    @Override
    public int sizeObjects()
    {
        return transactionObjectCollection.sizeObjects();
    }
}
