package com.linbit.linstor.transaction;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.K8sResourceClient;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorVersionCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackCrd;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;

import java.net.HttpURLConnection;
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
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

public class ControllerK8sCrdTransactionMgr implements TransactionMgrK8sCrd
{
    private static final Object SYNC_OBJ = new Object();

    private final TransactionObjectCollection transactionObjectCollection;
    private final ControllerK8sCrdDatabase controllerK8sCrdDatabase;

    private K8sCrdTransaction currentTransaction;

    private final Map<DatabaseTable, Supplier<K8sResourceClient<?>>> crdClientLut;
    private final MixedOperation<RollbackCrd, KubernetesResourceList<RollbackCrd>, Resource<RollbackCrd>> rollbackClient;
    private final MixedOperation<LinstorVersionCrd, KubernetesResourceList<LinstorVersionCrd>, Resource<LinstorVersionCrd>> linstorVersionClient;
    private final KubernetesClient k8sClient;
    private final String crdVersion;

    public ControllerK8sCrdTransactionMgr(ControllerK8sCrdDatabase controllerK8sCrdDatabaseRef)
    {
        this(
            controllerK8sCrdDatabaseRef,
            new BaseControllerK8sCrdTransactionMgrContext(
                GenCrdCurrent::databaseTableToCustomResourceClass,
                GeneratedDatabaseTables.ALL_TABLES,
                GenCrdCurrent.VERSION
            )
        );
    }

    public ControllerK8sCrdTransactionMgr(
        ControllerK8sCrdDatabase controllerK8sCrdDatabaseRef,
        BaseControllerK8sCrdTransactionMgrContext ctx
    )
    {
        controllerK8sCrdDatabase = controllerK8sCrdDatabaseRef;
        transactionObjectCollection = new TransactionObjectCollection();

        k8sClient = controllerK8sCrdDatabaseRef.getClient();

        crdClientLut = new HashMap<>();
        Function<DatabaseTable, Class<? extends LinstorCrd<? extends LinstorSpec<?, ?>>>> dbTableToCrdClass = ctx
            .getDbTableToCrdClass();
        crdVersion = ctx.getCrdVersion();
        for (DatabaseTable tbl : ctx.getAllDatabaseTables())
        {
            Class<? extends LinstorCrd<? extends LinstorSpec<?, ?>>> clazz = dbTableToCrdClass.apply(tbl);
            if (clazz != null)
            {
                /*
                 * otherwise GeneratedDatabaseTables.ALL_TABLES contains a table that the current version of the db /
                 * migration simply does not know. It should be safe to ignore this case, since the migration will most
                 * likely not try to access a table it does not know?
                 */
                crdClientLut.put(
                    tbl,
                    () -> controllerK8sCrdDatabaseRef.getCachingClient(clazz)
                );
            }
        }
        rollbackClient = k8sClient.resources(RollbackCrd.class);
        linstorVersionClient = k8sClient.resources(LinstorVersionCrd.class);

        currentTransaction = createNewTx();
    }

    private K8sCrdTransaction createNewTx()
    {
        return new K8sCrdTransaction(
            crdClientLut,
            rollbackClient,
            linstorVersionClient,
            crdVersion
        );
    }

    public @Nullable Integer getDbVersion()
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
        synchronized (SYNC_OBJ)
        {
            /*
             * We need to synchronize to prevent other threads to also start a new rollback entry but also to prevent
             * other to rollback a transaction
             */
            try
            {
                ControllerK8sCrdRollbackMgr.createRollbackEntry(
                    currentTransaction, controllerK8sCrdDatabase.getMaxRollbackEntries());
            }
            catch (DatabaseException exc)
            {
                throw new TransactionException("Error creating rollback entry", exc);
            }

            for (Entry<DatabaseTable, HashMap<String, LinstorCrd<?>>> entry : currentTransaction.rscsToCreate
                .entrySet())
            {
                create(entry.getKey(), entry.getValue());
            }
            for (Entry<DatabaseTable, HashMap<String, LinstorCrd<?>>> entry : currentTransaction.rscsToReplace
                .entrySet())
            {
                replace(entry.getKey(), entry.getValue());
            }
            for (Entry<DatabaseTable, HashMap<String, LinstorCrd<?>>> entry : currentTransaction.rscsToDelete
                .entrySet())
            {
                delete(entry.getKey(), entry.getValue());
            }

            transactionObjectCollection.commitAll();

            clearTransactionObjects();

            K8sCrdTransaction transactionToClean = currentTransaction;
            currentTransaction = createNewTx();

            ControllerK8sCrdRollbackMgr.cleanup(transactionToClean);
        }
    }

    @SuppressWarnings("unchecked")
    private <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> void create(
        DatabaseTable dbTableRef,
        HashMap<String, LinstorCrd<?>> changedCrds
    )
    {
        K8sResourceClient<CRD> client = currentTransaction.getClient(dbTableRef);
        for (LinstorCrd<?> linstorCrd : changedCrds.values())
        {
            client.create((CRD) linstorCrd);
        }
    }

    @SuppressWarnings("unchecked")
    private <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> void replace(
        DatabaseTable dbTableRef,
        HashMap<String, LinstorCrd<?>> changedCrds
    )
    {
        K8sResourceClient<CRD> client = currentTransaction
            .getClient(dbTableRef);
        for (LinstorCrd<?> linstorCrd : changedCrds.values())
        {
            client.replace((CRD) linstorCrd);
        }
    }

    @SuppressWarnings("unchecked")
    private <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> void delete(
        DatabaseTable dbTableRef,
        HashMap<String, LinstorCrd<?>> createdCrds
    )
    {
        K8sResourceClient<CRD> client = currentTransaction
            .getClient(dbTableRef);
        for (LinstorCrd<?> linstorCrd : createdCrds.values())
        {
            client.delete((CRD) linstorCrd);
        }
    }

    @Override
    public void rollback() throws TransactionException
    {
        synchronized (SYNC_OBJ)
        {
            ControllerK8sCrdRollbackMgr.rollback(currentTransaction, crdClientLut);

            transactionObjectCollection.rollbackAll();

            currentTransaction = createNewTx();

            clearTransactionObjects();
        }
    }

    public void rollbackIfNeeded() throws TransactionException
    {
        try
        {
            List<RollbackCrd> rollbacks = currentTransaction.getRollbackClient().list().getItems();
            if (!rollbacks.isEmpty())
            {
                currentTransaction.setRollbacks(rollbacks);

                rollback();
            }
        }
        catch (KubernetesClientException exc)
        {
            // This could just mean that no rollback CRD was applied, so there can also be no rollback.
            if (exc.getCode() != HttpURLConnection.HTTP_NOT_FOUND)
            {
                throw exc;
            }
        }
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
