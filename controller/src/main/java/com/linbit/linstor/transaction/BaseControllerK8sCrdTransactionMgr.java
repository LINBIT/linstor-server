package com.linbit.linstor.transaction;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorVersion;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackCrd;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

public class BaseControllerK8sCrdTransactionMgr<ROLLBACK_CRD extends RollbackCrd>
    implements TransactionMgrK8sCrd
{
    private final TransactionObjectCollection transactionObjectCollection;
    private final ControllerK8sCrdDatabase controllerK8sCrdDatabase;

    private K8sCrdTransaction<ROLLBACK_CRD> currentTransaction;

    private final Map<DatabaseTable, MixedOperation<?, ?, ?>> crdClientLut;
    private final MixedOperation<ROLLBACK_CRD, KubernetesResourceList<ROLLBACK_CRD>, Resource<ROLLBACK_CRD>> rollbackClient;
    private final MixedOperation<LinstorVersion, KubernetesResourceList<LinstorVersion>, Resource<LinstorVersion>> linstorVersionClient;
    private final Supplier<ROLLBACK_CRD> rollbackCrdSupplier;
    private final Function<LinstorSpec, LinstorCrd<LinstorSpec>> specToCrd;
    private final KubernetesClient k8sClient;

    public static BaseControllerK8sCrdTransactionMgr<GenCrdCurrent.Rollback> getDefault(
        ControllerK8sCrdDatabase controllerK8sCrdDatabaseRef
    )
    {
        return new BaseControllerK8sCrdTransactionMgr<>(
            controllerK8sCrdDatabaseRef,
            new BaseControllerK8sCrdTransactionMgrContext<>(
                GenCrdCurrent::databaseTableToCustomResourceClass,
                GenCrdCurrent.Rollback.class,
                GenCrdCurrent::newRollbackCrd,
                GenCrdCurrent::specToCrd
            )
        );
    }

    public BaseControllerK8sCrdTransactionMgr(
        ControllerK8sCrdDatabase controllerK8sCrdDatabaseRef,
        BaseControllerK8sCrdTransactionMgrContext<ROLLBACK_CRD> ctx
    )
    {
        controllerK8sCrdDatabase = controllerK8sCrdDatabaseRef;
        rollbackCrdSupplier = ctx.getRollbackCrdSupplier();
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
        rollbackClient = k8sClient.resources(ctx.getRollbackClass());
        linstorVersionClient = k8sClient.resources(LinstorVersion.class);

        currentTransaction = createNewTx();
    }

    private K8sCrdTransaction<ROLLBACK_CRD> createNewTx()
    {
        return new K8sCrdTransaction<>(crdClientLut, rollbackClient, linstorVersionClient);
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
                if (crd.getStatus().getAcceptedNames().getPlural().equals(LinstorVersion.LINSTOR_CRD_NAME))
                {
                    // k8s knows about the schema, we still need to get the actual linstorversion instance
                    List<LinstorVersion> linstorVersionList = linstorVersionClient.list().getItems();
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
    public K8sCrdTransaction<ROLLBACK_CRD> getTransaction()
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
            ControllerK8sCrdRollbackMgr.createRollbackEntry(
                currentTransaction,
                rollbackCrdSupplier.get()
            );
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
