package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

@EtcdMigration(
    description = "Fix duplicated restored layer data",
    version = 43
)
public class Migration_16_FixDuplicatedRestoredLayerData extends BaseEtcdMigration
{
    private static final String NODE_NAME = "NODE_NAME";
    private static final String RESOURCE_NAME = "RESOURCE_NAME";
    private static final String SNAPSHOT_NAME = "SNAPSHOT_NAME";
    private static final String KIND = "LAYER_RESOURCE_KIND";

    /*
     * 1.12.0 introduced a bug where restoring a snapshot into a new resource by accident creates too many layer-data.
     * The resource itself will work as expected, but when trying to remove the resource, the additionally created
     * layer-data will cause constraint violation exceptions as they still have foreign keys to the resource.
     */

    @Override
    public void migrate(EtcdTransaction tx, String prefix) throws Exception
    {
        TreeMap<String, LriKey> idToKey = new TreeMap<>();
        {
            TreeMap<String, String> map = tx.get(prefix + "LAYER_RESOURCE_IDS/", true);
            for (Entry<String, String> entry : map.entrySet())
            {
                String pk = extractPrimaryKey(entry.getKey());
                String columnName = getColumnName(entry.getKey());

                LriKey lriKey = idToKey.get(pk);
                if (lriKey == null)
                {
                    lriKey = new LriKey();
                    idToKey.put(pk, lriKey);
                }
                switch (columnName)
                {
                    case NODE_NAME:
                        lriKey.nodeName = entry.getValue();
                        break;
                    case RESOURCE_NAME:
                        lriKey.rscName = entry.getValue();
                        break;
                    case SNAPSHOT_NAME:
                        String snap = entry.getValue();
                        if (snap != null && !snap.isEmpty())
                        {
                            lriKey.snapName = snap;
                        }
                        break;
                    case KIND:
                        lriKey.kind = entry.getValue();
                        break;
                    default:// ignore the other columns
                }
            }
        }
        HashSet<RscKey> existingResourceSet = new HashSet<>();
        {
            TreeMap<String, String> map = tx.get(prefix + "RESOURCES/", true);
            for (String etcdKey : map.keySet())
            {
                String pk = extractPrimaryKey(etcdKey);
                String[] pks = EtcdUtils.splitPks(pk, false);
                if (pks[2] != null && pks[2].trim().isEmpty())
                {
                    pks[2] = null; // snapshot name
                }
                existingResourceSet.add(new RscKey(pks[0], pks[1], pks[2]));
            }
        }

        HashMap<LriKey, String> lastIdMap = new HashMap<>();
        for (Entry<String, LriKey> lriEntry : idToKey.entrySet())
        {
            LriKey lriKey = lriEntry.getValue();
            String currentLriId = lriEntry.getKey();

            String lastId = lastIdMap.put(lriKey, currentLriId);
            if (!existingResourceSet.contains(new RscKey(lriKey.nodeName, lriKey.rscName, lriKey.snapName)))
            {
                // the resource is already deleted, but we still have orphaned layer-data. get rid of those
                lastId = currentLriId;
            }

            if (lastId != null)
            {
                ArrayList<String> tablesToClean = new ArrayList<>();
                switch (lriKey.kind)
                {
                    case "CACHE":
                        tablesToClean.add("LAYER_CACHE_VOLUMES");
                        break;
                    case "DRBD":
                        tablesToClean.add("LAYER_DRBD_VOLUMES");
                        tablesToClean.add("LAYER_DRBD_RESOURCES");
                        break;
                    case "LUKS":
                        tablesToClean.add("LAYER_LUKS_VOLUMES");
                        break;
                    case "OPENFLEX":
                        tablesToClean.add("LAYER_OPENFLEX_VOLUMES");
                        break;
                    case "STORAGE":
                        tablesToClean.add("LAYER_STORAGE_VOLUMES");
                        break;
                    case "WRITECACHE":
                        tablesToClean.add("LAYER_WRITECACHE_VOLUMES");
                        break;
                    case "BCACHE":
                        tablesToClean.add("LAYER_BCACHE_VOLUMES");
                        break;
                    case "NVME":
                        // noop
                        break;
                    default:
                        throw new ImplementationError("Unknown kind: " + lriKey.kind);
                }
                tablesToClean.add("LAYER_RESOURCE_IDS");
                for (String tableToClean : tablesToClean)
                {
                    String etcdKeyToDelete = prefix + tableToClean + "/" + lastId;
                    tx.delete(etcdKeyToDelete, true);
                }
            }
        }
    }

    private static class LriKey
    {
        @Nullable String nodeName;
        @Nullable String rscName;
        @Nullable String snapName;
        @Nullable String kind;

        LriKey()
        {
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((kind == null) ? 0 : kind.hashCode());
            result = prime * result + ((nodeName == null) ? 0 : nodeName.hashCode());
            result = prime * result + ((rscName == null) ? 0 : rscName.hashCode());
            result = prime * result + ((snapName == null) ? 0 : snapName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (getClass() != obj.getClass())
            {
                return false;
            }
            LriKey other = (LriKey) obj;

            return Objects.equals(nodeName, other.nodeName) &&
                Objects.equals(rscName, other.rscName) &&
                Objects.equals(snapName, other.snapName) &&
                Objects.equals(kind, other.kind);
        }
    }

    private static class RscKey
    {
        String nodeName;
        String rscName;
        String snapName;

        public RscKey(String nodeNameRef, String rscNameRef, String snapNameRef)
        {
            nodeName = nodeNameRef;
            rscName = rscNameRef;
            snapName = snapNameRef;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((nodeName == null) ? 0 : nodeName.hashCode());
            result = prime * result + ((rscName == null) ? 0 : rscName.hashCode());
            result = prime * result + ((snapName == null) ? 0 : snapName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (getClass() != obj.getClass())
            {
                return false;
            }
            RscKey other = (RscKey) obj;
            return Objects.equals(nodeName, other.nodeName) &&
                Objects.equals(rscName, other.rscName) &&
                Objects.equals(snapName, other.snapName);
        }

    }
}
