package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map.Entry;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class Migration_07_PrefixLinstorNamespaceWithSlash extends EtcdMigration
{
    public static void migrate(EtcdTransaction tx) throws JsonMappingException, JsonProcessingException
    {
        TreeMap<String, String> allKeys = tx.get("LINSTOR/", true);
        for (Entry<String, String> entry : allKeys.entrySet())
        {
            tx.put("/" + entry.getKey(), entry.getValue());
        }
        tx.delete("LINSTOR/", true);
    }
}
