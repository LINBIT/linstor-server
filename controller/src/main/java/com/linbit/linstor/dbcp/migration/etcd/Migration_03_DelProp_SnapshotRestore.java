package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map.Entry;
import java.util.TreeMap;

@EtcdMigration(
    description = "Delete snapshot restore properties",
    version = 3
)
// corresponds to Migration_2019_10_31_SnapRestoreDeleteProps2
public class Migration_03_DelProp_SnapshotRestore extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx)
    {
        TreeMap<String, String> props = tx.get("LINSTOR/PROPS_CONTAINER/volumes", true);

        for (Entry<String, String> entry : props.entrySet())
        {
            if (entry.getKey().endsWith("PROP_KEY") &&
                (
                    entry.getValue().endsWith("RestoreFromResource") ||
                    entry.getValue().endsWith("RestoreFromSnapshot")
                )
            )
            {
                tx.delete(entry.getKey(), false);
            }
        }
    }

    @Override
    public int getNextVersion()
    {
        // we introduced a bug where instead of writing (3+1) we accidentally written
        // ("3" + "1").
        return 31;
    }
}
