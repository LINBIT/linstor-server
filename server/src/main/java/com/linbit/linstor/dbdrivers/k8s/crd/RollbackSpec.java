package com.linbit.linstor.dbdrivers.k8s.crd;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.DatabaseTable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;

public class RollbackSpec implements LinstorSpec<RollbackCrd, RollbackSpec>
{
    @JsonIgnore
    private static final long serialVersionUID = -4327494414912303196L;

    @JsonIgnore
    private final Object syncObject = new Object();

    /** Contains original data for each modified or created Spec class */
    // HashMap<dbTable.toString(), HashMap<spec.getKey(), spec>>
    @JsonProperty("rollback_map")
    public final HashMap<String, HashMap<String, GenericKubernetesResource>> rollbackMap;

    /**
     * Contains keys of instances that were created in this transaction.
     * A rollback causes deletion of these entries
     */
    // HashMap<dbTable.toString(), HashMap<spec.getKey(), spec>>
    @JsonProperty("delete_map")
    public final HashMap<String, HashSet<String>> deleteMap;

    RollbackCrd parentCrd;

    public RollbackSpec()
    {
        rollbackMap = new HashMap<>();
        deleteMap = new HashMap<>();
    }

    @JsonIgnore
    @Override
    public RollbackCrd getCrd()
    {
        return parentCrd;
    }

    @JsonCreator
    public RollbackSpec(
        @JsonProperty("rollback_map") HashMap<String, HashMap<String, GenericKubernetesResource>> rollbackMapRef,
        @JsonProperty("delete_map") HashMap<String, HashSet<String>> deleteMapRef
    )
    {
        rollbackMap = rollbackMapRef == null ? new HashMap<>() : rollbackMapRef;
        deleteMap = deleteMapRef == null ? new HashMap<>() : deleteMapRef;
    }

    @JsonIgnore
    public static String getYamlLocation()
    {
        return "com/linbit/linstor/dbcp/k8s/crd/Rollback.yaml";
    }

    @JsonIgnore
    public <SPEC extends LinstorSpec> void updatedOrDeleted(DatabaseTable dbTable, LinstorCrd<SPEC> crd)
    {
        synchronized (syncObject)
        {
            String dbTableStr = dbTable.getName();
            String specKey = crd.getK8sKey();
            if (!alreadyKnown(dbTableStr, specKey))
            {
                HashMap<String, GenericKubernetesResource> rbMap = rollbackMap.get(dbTableStr);
                if (rbMap == null)
                {
                    rbMap = new HashMap<>();
                    rollbackMap.put(dbTableStr, rbMap);
                }
                GenericKubernetesResource gkr = new GenericKubernetesResource();
                gkr.setAdditionalProperty("spec", crd.getSpec());
                gkr.setMetadata(crd.getMetadata());
                gkr.setKind(crd.getKind());
                gkr.setApiVersion(crd.getApiVersion());
                rbMap.put(specKey, gkr);
            }
        }
    }

    @JsonIgnore
    public void created(DatabaseTable dbTable, String specKey)
    {
        synchronized (syncObject)
        {
            String dbTableStr = dbTable.getName();
            if (!alreadyKnown(dbTableStr, specKey))
            {
                HashSet<String> delSet = deleteMap.get(dbTableStr);
                if (delSet == null)
                {
                    delSet = new HashSet<>();
                    deleteMap.put(dbTableStr, delSet);
                }
                delSet.add(specKey);
            }
        }
    }

    public HashMap<String, HashMap<String, GenericKubernetesResource>> getRollbackMap()
    {
        return rollbackMap;
    }

    public HashMap<String, HashSet<String>> getDeleteMap()
    {
        return deleteMap;
    }

    @JsonIgnore
    private boolean alreadyKnown(String dbTableStr, String specKey)
    {
        HashMap<String, GenericKubernetesResource> rbMap = rollbackMap.get(dbTableStr);
        HashSet<String> delSet = deleteMap.get(dbTableStr);
        return (rbMap != null && rbMap.containsKey(specKey)) || (delSet != null && delSet.contains(specKey));
    }

    @JsonIgnore
    @Override
    public String getLinstorKey()
    {
        return "rollback";
    }

    @Override
    @JsonIgnore
    public Map<String, Object> asRawParameters()
    {
        throw new ImplementationError("Method not supported by Rollback");
    }

    @Override
    @JsonIgnore
    public Object getByColumn(String clmNameStrRef)
    {
        throw new ImplementationError("Method not supported by Rollback");
    }

    @Override
    @JsonIgnore
    public DatabaseTable getDatabaseTable()
    {
        throw new ImplementationError("Method not supported by Rollback");
    }
}
