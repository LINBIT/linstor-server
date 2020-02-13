package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map.Entry;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

@EtcdMigration(
    description = "Prefix all linstor etcd keys with /",
    version = 34
)
public class Migration_07_PrefixLinstorNamespaceWithSlash extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx) throws JsonMappingException, JsonProcessingException
    {
        TreeMap<String, String> allKeys = tx.get("LINSTOR/", true);
        for (Entry<String, String> entry : allKeys.entrySet())
        {
            tx.put("/" + entry.getKey(), entry.getValue());
        }
        tx.delete("LINSTOR/", true);
    }
}
