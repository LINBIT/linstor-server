package com.linbit.linstor.transaction;

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

    final HashMap<DatabaseTable, HashMap<String, LinstorCrd<?>>> rscsToChangeOrCreate;
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

        rscsToChangeOrCreate = new HashMap<>();
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

    public void update(DatabaseTable dbTable, LinstorCrd<?> k8sRsc)
    {
        // System.out.println("updating " + dbTable.getName() + " " + k8sRsc);
        String key = k8sRsc.getK8sKey();
        lazyGet(rscsToChangeOrCreate, dbTable).put(key, k8sRsc);
        lazyGet(rscsToDelete, dbTable).remove(key);
    }

    public void delete(DatabaseTable dbTable, LinstorCrd<?> k8sRsc)
    {
        // System.out.println("deleting entry from " + dbTable.getName() + ": " + k8sRsc);
        String key = k8sRsc.getK8sKey();
        lazyGet(rscsToDelete, dbTable).put(key, k8sRsc);
        lazyGet(rscsToChangeOrCreate, dbTable).remove(key);
    }

    private HashMap<String, LinstorCrd<?>> lazyGet(
        HashMap<DatabaseTable, HashMap<String, LinstorCrd<?>>> map,
        DatabaseTable dbTableRef
    )
    {
        HashMap<String, LinstorCrd<?>> ret = map.get(dbTableRef);
        if (ret == null)
        {
            ret = new HashMap<>();
            map.put(dbTableRef, ret);
        }
        return ret;
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
