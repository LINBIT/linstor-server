package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

@EtcdMigration(
    description = "Default replica count for resource groups",
    version = 33
)
public class Migration_06_RscGrpDfltReplCount extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx) throws JsonMappingException, JsonProcessingException
    {
        TreeMap<String, String> allRscDfn = tx.get("LINSTOR/RESOURCE_GROUPS", true);

        HashSet<String> rscGrpWithoutReplicaCount = new HashSet<>();
        HashSet<String> rscGrpWithReplicaCount = new HashSet<>();
        for (Entry<String, String> entry : allRscDfn.entrySet())
        {
            String key = entry.getKey();
            String rscGrpName = extractPrimaryKey(key);
            if (!rscGrpWithReplicaCount.contains(rscGrpName))
            {
                if (key.endsWith("REPLICA_COUNT"))
                {
                    rscGrpWithReplicaCount.add(rscGrpName);
                    rscGrpWithoutReplicaCount.remove(rscGrpName);
                }
                else
                {
                    rscGrpWithoutReplicaCount.add(rscGrpName);
                }
            }
        }

        for (String rscGrpName : rscGrpWithoutReplicaCount)
        {
            tx.put("LINSTOR/RESOURCE_GROUPS/" + rscGrpName + "/REPLICA_COUNT", "2");
        }
    }
}
