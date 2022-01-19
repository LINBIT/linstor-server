package com.linbit.linstor.dbdrivers.k8s.crd;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RollbackSpec implements LinstorSpec
{
    @JsonIgnore
    private static final long serialVersionUID = -4327494414912303196L;

    @JsonIgnore
    private final Object syncObject = new Object();

    /** Contains original data for each modified or created Spec class */
    // HashMap<dbTable.toString(), HashMap<spec.getKey(), spec>>
    @JsonProperty("rollbackMap")
    public final HashMap<String, HashMap<String, LinstorSpec>> rollbackMap;

    /**
     * Contains keys of instances that were created in this transaction.
     * A rollback causes deletion of these entries
     */
    // HashMap<dbTable.toString(), HashMap<spec.getKey(), spec>>
    @JsonProperty("deleteMap")
    public final HashMap<String, HashSet<String>> deleteMap;

    public RollbackSpec()
    {
        rollbackMap = new HashMap<>();
        deleteMap = new HashMap<>();
    }

    @JsonCreator
    public RollbackSpec(
        @JsonProperty("rollbackMap") HashMap<String, HashMap<String, LinstorSpec>> rollbackMapRef,
        @JsonProperty("deleteMap") HashMap<String, HashSet<String>> deleteMapRef
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
            String dbTableStr = dbTable.toString();
            String specKey = crd.getK8sKey();
            if (!alreadyKnown(dbTableStr, specKey))
            {
                HashMap<String, LinstorSpec> rbMap = rollbackMap.get(dbTableStr);
                if (rbMap == null)
                {
                    rbMap = new HashMap<>();
                    rollbackMap.put(dbTableStr, rbMap);
                }
                rbMap.put(specKey, crd.getSpec());
            }
        }
    }

    @JsonIgnore
    public void created(DatabaseTable dbTable, String specKey)
    {
        synchronized (syncObject)
        {
            String dbTableStr = dbTable.toString();
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

    public HashMap<String, HashMap<String, LinstorSpec>> getRollbackMap()
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
        HashMap<String, LinstorSpec> rbMap = rollbackMap.get(dbTableStr);
        HashSet<String> delSet = deleteMap.get(dbTableStr);
        return (rbMap == null || !rbMap.containsKey(specKey)) && (delSet == null || !delSet.contains(specKey));
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
    public Object getByColumn(Column clmRef)
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
