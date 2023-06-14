package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map.Entry;
import java.util.TreeMap;

@EtcdMigration(
    description = "Upper case props instance",
    version = 54
)
public class Migration_27_UpperCasePropsInstance extends BaseEtcdMigration
{
    private static final String TBL = "PROPS_CONTAINERS/";

    private static final String PK_DELIMITER = ":";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        String base = prefix + TBL;
        int baseKeyLen = base.length();
        TreeMap<String, String> props = tx.get(base);

        for (Entry<String, String> entry : props.entrySet())
        {
            String oldFullEtcdKey = entry.getKey();
            int propKeyStart = oldFullEtcdKey.indexOf(PK_DELIMITER, baseKeyLen);
            String instanceName = oldFullEtcdKey.substring(
                baseKeyLen,
                propKeyStart
            );
            String newEtcdKey = base + instanceName.toUpperCase() + oldFullEtcdKey.substring(propKeyStart);

            tx.delete(oldFullEtcdKey, false);
            tx.put(newEtcdKey, entry.getValue());
        }
    }
}
