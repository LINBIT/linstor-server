package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.HashSet;
import java.util.TreeMap;
import java.util.UUID;

@EtcdMigration(
    description = "Restore invisible KVS",
    version = 59
)
public class Migration_32_RestoreInvisibleKvs extends BaseEtcdMigration
{
    /*
     * Migration_23_CleanupOrphanedObjects accidentally deleted all KVS entries from the KVS table, but not
     * the props.
     * The problem was that the migration scanned for KVS with no properties, but was looking for lower-cased
     * PROPS_INSTANCE entries although the driver uppercases the PROPS_INSTANCE always.
     *
     * The props were not touched, but since the entries in the KVS table was missing,
     * "linstor key-value-store list" did not list them and "linstor key-value-store show <kvs>" also did not
     * contain any entries since they were simply not loaded from the database
     */


    private static final String TBL_PROPS = "PROPS_CONTAINERS/";
    private static final String TBL_KVS = "KEY_VALUE_STORE/";

    private static final String CLM_PROPS_INSTANCE = "PROPS_INSTANCE";

    private static final String CLM_KVS_NAME = "KVS_NAME";
    private static final String CLM_KVS_UUID = "UUID";
    private static final String CLM_KVS_DSP_NAME = "KVS_DSP_NAME";

    private static final String PROP_INSTANCE_PREFIX = "/KEYVALUESTORES/";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        String prefixedPropsTblKey = prefix + TBL_PROPS + PROP_INSTANCE_PREFIX;
        String prefixedKvsTblKey = prefix + TBL_KVS;
        int prefixedPropsTblKeyLen = prefixedPropsTblKey.length();
        int prefixedKvsTblKeyLen = prefixedKvsTblKey.length();

        HashSet<String> kvsFromProps = new HashSet<>();

        TreeMap<String, String> props = tx.get(prefixedPropsTblKey);
        for (String fullPropsEtcdKey : props.keySet())
        {
            String propsInstanceAndKey = fullPropsEtcdKey.substring(prefixedPropsTblKeyLen);
            String propsInstance = propsInstanceAndKey.substring(0, propsInstanceAndKey.indexOf(":"));

            kvsFromProps.add(propsInstance);
        }

        TreeMap<String, String> kvs = tx.get(prefixedKvsTblKey);
        for (String fullPropsEtcdKey : kvs.keySet())
        {
            String pkAndClm = fullPropsEtcdKey.substring(prefixedKvsTblKeyLen);
            String pk = pkAndClm.substring(pkAndClm.indexOf("/"));

            kvsFromProps.remove(pk);
        }

        for (String kvsToRestore : kvsFromProps)
        {
            tx.put(prefixedKvsTblKey + kvsToRestore + "/UUID", UUID.randomUUID().toString());
            tx.put(prefixedKvsTblKey + kvsToRestore + "/KVS_DSP_NAME", kvsToRestore.toLowerCase());
        }
    }
}
