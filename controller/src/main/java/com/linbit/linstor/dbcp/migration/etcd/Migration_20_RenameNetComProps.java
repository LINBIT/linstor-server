package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbcp.migration.Migration_2022_03_23_RenameNetComProps;
import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map.Entry;
import java.util.TreeMap;

@EtcdMigration(
    description = "Rename netcom to NetCom namespace",
    version = 47
)
public class Migration_20_RenameNetComProps extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx, String prefix) throws Exception
    {
        String ctrlConfEtcdBaseKey = prefix + "PROPS_CONTAINERS//CTRLCFG:";
        String netcomEtcdBaseKey = ctrlConfEtcdBaseKey + "netcom";
        TreeMap<String, String> treeMap = tx.get(netcomEtcdBaseKey, true);
        for (Entry<String, String> entry : treeMap.entrySet())
        {
            String oldFullEtcdKey = entry.getKey();

            String oldPropKey = oldFullEtcdKey.substring(ctrlConfEtcdBaseKey.length());
            String newPropkey = Migration_2022_03_23_RenameNetComProps.getNewKey(oldPropKey);

            String valueNew = Migration_2022_03_23_RenameNetComProps.getNewValue(oldPropKey, entry.getValue());

            System.out.println("removing old key: " + oldFullEtcdKey);
            tx.delete(oldFullEtcdKey, false);
            System.out.println("adding new key: " + ctrlConfEtcdBaseKey + newPropkey + " " + valueNew);
            tx.put(ctrlConfEtcdBaseKey + newPropkey, valueNew);
        }
    }
}
