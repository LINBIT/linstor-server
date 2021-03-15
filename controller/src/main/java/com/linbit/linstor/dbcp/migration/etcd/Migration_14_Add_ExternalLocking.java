package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

@EtcdMigration(
    description = "Add 'external_locking' column, with default FALSE to storage pools",
    version = 41
)
public class Migration_14_Add_ExternalLocking extends BaseEtcdMigration
{
    private static final String COL_EXTERNAL_LOCKING = "/EXTERNAL_LOCKING";
    private static final String TBL_STOR_POOL = "NODE_STOR_POOL/";
    private static final String DFLT_VAL_FALSE = "false";

    @Override
    public void migrate(EtcdTransaction tx, String prefix) throws Exception
    {
        TreeMap<String, String> keyVal = tx.get(prefix + TBL_STOR_POOL, true);

        Set<String> processedPks = new HashSet<>();
        for (Entry<String, String> entry : keyVal.entrySet())
        {
            String fullKey = entry.getKey();
            String pk = extractPrimaryKey(fullKey);
            if (processedPks.add(pk))
            { // only true if set did not already contain the specified element
                tx.put(prefix + TBL_STOR_POOL + pk + COL_EXTERNAL_LOCKING, DFLT_VAL_FALSE);
            }
        }
    }

}
