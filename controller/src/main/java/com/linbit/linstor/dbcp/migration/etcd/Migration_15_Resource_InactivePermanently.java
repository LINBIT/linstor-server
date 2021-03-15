package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map.Entry;
import java.util.TreeMap;

@EtcdMigration(
    description = "Additionally set INACTIVE_PERMANENTLY flag to INACTIVE DRBD resources",
    version = 42
)
public class Migration_15_Resource_InactivePermanently extends BaseEtcdMigration
{
    private static final String RESOURCE_FLAGS = "RESOURCE_FLAGS";

    private static final String KIND = "LAYER_RESOURCE_KIND";
    private static final String NODE_NAME = "NODE_NAME";
    private static final String RSC_NAME = "RESOURCE_NAME";
    private static final String DRBD = "DRBD";

    private static final long INACTIVE_BIT = 1L << 10;
    private static final long INACTIVE_PERMANENTLY_BITS = INACTIVE_BIT | 1L << 12;

    @Override
    public void migrate(EtcdTransaction tx, String prefix) throws Exception
    {
        TreeMap<String, String> map = tx.get(prefix + "LAYER_RESOURCE_IDS/", true);

        TreeMap<String, TreeMap<String, String>> lriByIds = new TreeMap<>();

        for (Entry<String, String> entry : map.entrySet())
        {
            String pk = extractPrimaryKey(entry.getKey());
            String columnName = getColumnName(entry.getKey());

            TreeMap<String, String> lri = lriByIds.get(pk);
            if (lri == null)
            {
                lri = new TreeMap<>();
                lriByIds.put(pk, lri);
            }
            lri.put(columnName, entry.getValue());
        }

        for (Entry<String, TreeMap<String, String>> lri : lriByIds.entrySet())
        {
            TreeMap<String, String> lriMap = lri.getValue();
            String kind = lriMap.get(KIND);
            if (kind.equalsIgnoreCase(DRBD))
            {
                String nodeName = lriMap.get(NODE_NAME);
                String rscName = lriMap.get(RSC_NAME);
                String rscFlagsKey = "/LINSTOR/RESOURCES/" + nodeName + ":" + rscName + ":/" + RESOURCE_FLAGS;
                String rscFlags = tx.getFirstValue(rscFlagsKey);

                long flags = Long.parseLong(rscFlags);
                if ((flags & INACTIVE_BIT) == INACTIVE_BIT)
                {
                    flags |= INACTIVE_PERMANENTLY_BITS;
                    tx.put(rscFlagsKey, Long.toString(flags));
                }
            }
        }
    }
}
