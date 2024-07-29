package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

@EtcdMigration(
    description = "Add empty snapshot name to LayerResourceIds",
    version = 53
)
public class Migration_26_AddEmptySnapNameToLRI extends BaseEtcdMigration
{
    private static final String CLM_NAME_SNAPSHOT_NAME = "SNAPSHOT_NAME";
    private static final String BASE_KEY = "LAYER_RESOURCE_IDS/";
    private static final String DFLT_SNAP_NAME_FOR_RSC = "";

    private static final String PATH_DELIMITER = "/";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        Map<Integer, Lri> lriMap = extractLri(tx.get(prefix + BASE_KEY));

        String etcdKeyFormat = prefix + BASE_KEY + "%d" + PATH_DELIMITER + CLM_NAME_SNAPSHOT_NAME;
        for (Lri lri : lriMap.values())
        {
            if (lri.snapName == null)
            {
                tx.put(String.format(etcdKeyFormat, lri.id), DFLT_SNAP_NAME_FOR_RSC);
            }
        }
    }

    private Map<Integer, Lri> extractLri(Map<String, String> etcdMap)
    {
        Map<Integer, Lri> ret = new HashMap<>();
        int dfltOffset = BASE_KEY.length();

        for (Entry<String, String> entry : etcdMap.entrySet())
        {
            String fullEtcdKey = entry.getKey();
            int pkStart = fullEtcdKey.indexOf(BASE_KEY) + dfltOffset;
            String pkStr = fullEtcdKey.substring(pkStart, fullEtcdKey.indexOf(PATH_DELIMITER, pkStart + 1));
            int pk = Integer.parseInt(pkStr);

            Lri lri = ret.get(pk);
            if (lri == null)
            {
                lri = new Lri(pk);
                ret.put(pk, lri);
            }
            String columnName = fullEtcdKey.substring(
                fullEtcdKey.lastIndexOf(PATH_DELIMITER) + PATH_DELIMITER.length()
            );
            if (CLM_NAME_SNAPSHOT_NAME.equals(columnName))
            {
                // other columns are ignored
                lri.snapName = entry.getValue();
            }
        }
        return ret;
    }

    class Lri
    {
        int id;
        @Nullable String snapName;
        // other entries can be ignored

        public Lri(int idRef)
        {
            id = idRef;
        }
    }
}
