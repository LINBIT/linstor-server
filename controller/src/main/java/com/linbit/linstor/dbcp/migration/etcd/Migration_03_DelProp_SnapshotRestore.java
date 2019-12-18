package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.ControllerETCDTransactionMgr;

import java.util.Map.Entry;
import java.util.TreeMap;

// corresponds to Migration_2019_10_31_SnapRestoreDeleteProps2
public class Migration_03_DelProp_SnapshotRestore extends EtcdMigration
{
    public static void migrate(ControllerETCDTransactionMgr txMgr)
    {
        TreeMap<String, String> props = txMgr.readTable("LINSTOR/PROPS_CONTAINER/volumes", true);

        for (Entry<String, String> entry : props.entrySet())
        {
            if (entry.getKey().endsWith("PROP_KEY") &&
                (
                    entry.getValue().endsWith("RestoreFromResource") ||
                    entry.getValue().endsWith("RestoreFromSnapshot")
                )
            )
            {
                txMgr.getTransaction().delete(
                    delReq(entry.getKey(), false)
                );
            }
        }
    }
}
