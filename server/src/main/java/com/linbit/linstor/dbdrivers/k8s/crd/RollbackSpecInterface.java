package com.linbit.linstor.dbdrivers.k8s.crd;

import com.linbit.linstor.dbdrivers.DatabaseTable;

import java.util.HashMap;
import java.util.HashSet;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface RollbackSpecInterface extends LinstorSpec
{
    @JsonIgnore
    void updatedOrDeleted(DatabaseTable dbTable, LinstorSpec data);

    @JsonIgnore
    void created(DatabaseTable dbTable, String specKey);

    HashMap<String, HashSet<String>> getDeleteMap();

    HashMap<String, HashMap<String, LinstorSpec>> getRollbackMap();
}
