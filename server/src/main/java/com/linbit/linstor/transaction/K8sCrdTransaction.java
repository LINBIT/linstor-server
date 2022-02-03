package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorVersionCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorVersionSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackCrd;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

public class K8sCrdTransaction
{
    private final Map<DatabaseTable, MixedOperation<?, ?, ?>> crdClientLut;
    private final MixedOperation<RollbackCrd, KubernetesResourceList<RollbackCrd>, Resource<RollbackCrd>> rollbackClient;
    private final MixedOperation<LinstorVersionCrd, KubernetesResourceList<LinstorVersionCrd>, Resource<LinstorVersionCrd>> linstorVersionClient;
    private final String crdVersion;

    final HashMap<DatabaseTable, HashMap<String, LinstorCrd<?>>> rscsToCreate;
    final HashMap<DatabaseTable, HashMap<String, LinstorCrd<?>>> rscsToReplace;
    final HashMap<DatabaseTable, HashMap<String, LinstorCrd<?>>> rscsToDelete;

    public K8sCrdTransaction(
        Map<DatabaseTable, MixedOperation<?, ?, ?>> crdClientLutRef,
        MixedOperation<RollbackCrd, KubernetesResourceList<RollbackCrd>, Resource<RollbackCrd>> rollbackClientRef,
        MixedOperation<LinstorVersionCrd, KubernetesResourceList<LinstorVersionCrd>, Resource<LinstorVersionCrd>> linstorVersionClientRef,
        String crdVersionRef
    )
    {
        crdClientLut = crdClientLutRef;
        rollbackClient = rollbackClientRef;
        linstorVersionClient = linstorVersionClientRef;
        crdVersion = crdVersionRef;

        rscsToCreate = new HashMap<>();
        rscsToReplace = new HashMap<>();
        rscsToDelete = new HashMap<>();
    }

    public MixedOperation<RollbackCrd, KubernetesResourceList<RollbackCrd>, Resource<RollbackCrd>> getRollbackClient()
    {
        return rollbackClient;
    }

    public MixedOperation<LinstorVersionCrd, KubernetesResourceList<LinstorVersionCrd>, Resource<LinstorVersionCrd>> getLinstorVersionClient()
    {
        return linstorVersionClient;
    }

    public void updateLinstorVersion(int version)
    {
        LinstorVersionCrd linstorVersion = new LinstorVersionCrd(new LinstorVersionSpec(version));
        linstorVersionClient.createOrReplace(linstorVersion);
    }

    public static boolean hasClientValidCrd(MixedOperation<?, ?, ?> client)
    {
        boolean ret;
        try
        {
            client.list();
            ret = true;
        }
        catch (KubernetesClientException exc)
        {
            ret = false;
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> MixedOperation<CRD, KubernetesResourceList<CRD>, Resource<CRD>> getClient(
        DatabaseTable dbTable
    )
    {
        return (MixedOperation<CRD, KubernetesResourceList<CRD>, Resource<CRD>>) crdClientLut
            .get(dbTable);
    }

    public void createOrReplace(DatabaseTable dbTable, LinstorCrd<?> k8sRsc, boolean isNew)
    {
        if (isNew)
        {
            create(dbTable, k8sRsc);
        }
        else
        {
            replace(dbTable, k8sRsc);
        }
    }

    public void create(DatabaseTable dbTable, LinstorCrd<?> k8sRsc)
    {
        String key = k8sRsc.getK8sKey();

        boolean isCreated = lazyRemove(rscsToCreate, dbTable, key);
        boolean isReplaced = lazyRemove(rscsToReplace, dbTable, key);
        boolean isDeleted = lazyRemove(rscsToDelete, dbTable, key);

        if (isCreated || isReplaced)
        {
            throw new ImplementationError(
                "Tried to create resource " + k8sRsc.getKind() + "/" + key +
                    " that was already created or updated in the same transaction"
            );
        }

        if (isDeleted)
        {
            // Delete, then Create is merged to Update
            lazyGet(rscsToReplace, dbTable).put(key, k8sRsc);
        }
        else
        {
            lazyGet(rscsToCreate, dbTable).put(key, k8sRsc);
        }
    }

    public void replace(DatabaseTable dbTable, LinstorCrd<?> k8sRsc)
    {
        String key = k8sRsc.getK8sKey();

        boolean isCreated = lazyRemove(rscsToCreate, dbTable, key);
        boolean isReplaced = lazyRemove(rscsToReplace, dbTable, key);
        boolean isDeleted = lazyRemove(rscsToDelete, dbTable, key);

        if (isDeleted)
        {
            throw new ImplementationError(
                "Tried to update resource " + k8sRsc.getKind() + "/" + key +
                    " that was already deleted in the same transaction"
            );
        }

        if (isCreated)
        {
            // Create, then Update is merged to Create
            lazyGet(rscsToCreate, dbTable).put(key, k8sRsc);
        }
        else
        {
            lazyGet(rscsToReplace, dbTable).put(key, k8sRsc);
        }
    }

    public void delete(DatabaseTable dbTable, LinstorCrd<?> k8sRsc)
    {
        String key = k8sRsc.getK8sKey();

        boolean isCreated = lazyRemove(rscsToCreate, dbTable, key);
        boolean isReplaced = lazyRemove(rscsToReplace, dbTable, key);
        boolean isDeleted = lazyRemove(rscsToDelete, dbTable, key);

        // NB: deleting the same object multiple times is fine. As an example, properties might be deleted from multiple
        // sources.

        if (isCreated)
        {
            // Create, then Delete is a no-op
        }
        else
        {
            lazyGet(rscsToDelete, dbTable).put(key, k8sRsc);
        }
    }

    private HashMap<String, LinstorCrd<?>> lazyGet(
        HashMap<DatabaseTable, HashMap<String, LinstorCrd<?>>> map,
        DatabaseTable dbTableRef
    )
    {
        return map.computeIfAbsent(dbTableRef, (ignored) -> new HashMap<>());
    }

    private boolean lazyRemove(
        HashMap<DatabaseTable, HashMap<String, LinstorCrd<?>>> map,
        DatabaseTable dbTableRef,
        String key
    )
    {
        boolean removed = false;
        HashMap<String, LinstorCrd<?>> table = map.get(dbTableRef);
        if (table != null)
        {
            LinstorCrd<?> existing = table.remove(key);
            removed = existing != null;
        }

        return removed;
    }

    public <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> HashMap<String, SPEC> getSpec(DatabaseTable dbTable)
    {
        return this.<CRD, SPEC>getSpec(dbTable, ignored -> true);
    }

    public <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> HashMap<String, SPEC> getSpec(
        DatabaseTable dbTable,
        Predicate<CRD> matcher
    )
    {
        HashMap<String, SPEC> ret = new HashMap<>();
        MixedOperation<CRD, KubernetesResourceList<CRD>, Resource<CRD>> client = getClient(
            dbTable
        );
        ListOptionsBuilder builder = new ListOptionsBuilder();
        // builder.withApiVersion(crdVersion);
        KubernetesResourceList<CRD> list = client.list(builder.build());
        for (CRD item : list.getItems())
        {
            if (matcher.test(item))
            {
                ret.put(item.getK8sKey(), item.getSpec());
            }
        }
        return ret;
    }

    public <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> SPEC getSpec(
        DatabaseTable dbTable,
        Predicate<CRD> matcher,
        boolean failIfNullRef,
        String notFoundMessage
    )
        throws DatabaseException
    {
        SPEC ret = null;
        HashMap<String, SPEC> map = getSpec(dbTable, matcher);
        if (map.isEmpty())
        {
            if (failIfNullRef)
            {
                throw new DatabaseException(notFoundMessage);
            }
        }
        else if (map.size() > 1)
        {
            throw new DatabaseException("Duplicate entry for single get");
        }
        else
        {
            ret = map.values().iterator().next();
        }
        return ret;
    }

    public <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> HashMap<String, CRD> getCrd(DatabaseTable dbTable)
    {
        return this.<CRD, SPEC>getCrd(dbTable, ignored -> true);
    }

    public <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec> HashMap<String, CRD> getCrd(
        DatabaseTable dbTable,
        Predicate<CRD> matcher
    )
    {
        HashMap<String, CRD> ret = new HashMap<>();
        MixedOperation<CRD, KubernetesResourceList<CRD>, Resource<CRD>> client = getClient(
            dbTable
        );
        KubernetesResourceList<CRD> list = client.list();
        for (CRD item : list.getItems())
        {
            if (matcher.test(item))
            {
                ret.put(item.getK8sKey(), item);
            }
        }
        return ret;
    }
}
