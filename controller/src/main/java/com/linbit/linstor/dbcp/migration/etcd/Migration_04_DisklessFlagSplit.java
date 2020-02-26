package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

@EtcdMigration(
    description = "Split DISKLESS flag into DRBD_DISKLESS and NVME_INITIATOR",
    version = 31
)
// corresponds to Migration_2019_11_12_DisklessFlagSplit
public class Migration_04_DisklessFlagSplit extends BaseEtcdMigration
{
    private static final long FLAG_DISKLESS = 1L << 2;
    private static final long FLAG_DRBD_DISKLESS = FLAG_DISKLESS | 1L << 8;
    private static final long FLAG_NVME_INITIATOR = FLAG_DISKLESS | 1L << 9;

    private static final String NODE_NAME = "NODE_NAME";
    private static final String RSC_NAME = "RESOURCE_NAME";
    private static final String RSC_FLAGS = "RESOURCE_FLAGS";
    private static final String LAYER_RESOURCE_KIND = "LAYER_RESOURCE_KIND";

    private static final String KIND_DRBD = "DRBD";
    private static final String KIND_NVME = "NVME";

    @Override
    public void migrate(EtcdTransaction tx)
    {
        TreeMap<String, String> allRscLayer = tx.get("LINSTOR/LAYER_RESOURCE_IDS", true);

        // key: <nodeName, rscName>, value: <etcdKey, rscFlag>
        HashMap<Pair<String, String>, Pair<String, Long>> rscMap = new HashMap<>();
        {
            TreeMap<String, String> allRsc = tx.get("LINSTOR/RESOURCES", true);
            for (Entry<String, String> rsc : allRsc.entrySet())
            {
                String key = rsc.getKey();
                if (key.endsWith(RSC_FLAGS))
                {
                    String nodeName;
                    String rscName;
                    {
                        String composedKey = extractPrimaryKey(key);
                        String[] split = EtcdUtils.splitPks(composedKey, false);
                        nodeName = split[0];
                        rscName = split[1];
                    }
                    long flags = Long.parseLong(rsc.getValue());

                    rscMap.put(new Pair<>(nodeName, rscName), new Pair<>(key, flags));
                }
            }
        }

        HashMap<Long, LayerRscHolder> rscDataMap = new HashMap<>();
        for (Entry<String, String> rscLayer : allRscLayer.entrySet())
        {
            String etcdKey = rscLayer.getKey();
            long layerRscId = Long.parseLong(extractPrimaryKey(etcdKey));

            LayerRscHolder layerRscHolder = rscDataMap.get(layerRscId);
            {
                if (layerRscHolder == null) {
                    layerRscHolder = new LayerRscHolder();
                    rscDataMap.put(layerRscId, layerRscHolder);
                }
            }
            String columnName = getColumnName(etcdKey);
            switch (columnName)
            {
                case NODE_NAME:
                    layerRscHolder.nodeName = rscLayer.getValue();
                    break;
                case RSC_NAME:
                    layerRscHolder.rscName = rscLayer.getValue();
                    break;
                case LAYER_RESOURCE_KIND:
                    layerRscHolder.kind = rscLayer.getValue();
                    break;
            }
            if (layerRscHolder.isComplete())
            {
                Pair<String, Long> rscKeyAndFlag = rscMap.get(
                    new Pair<>(
                        layerRscHolder.nodeName,
                        layerRscHolder.rscName
                    )
                );
                long flag = rscKeyAndFlag.b;
                if ((flag & FLAG_DISKLESS) == FLAG_DISKLESS)
                {
                    boolean update = false;
                    if (layerRscHolder.kind.equals(KIND_DRBD))
                    {
                        flag |= FLAG_DRBD_DISKLESS;
                        update = true;
                    }
                    else
                    if (layerRscHolder.kind.equals(KIND_NVME))
                    {
                        flag |= FLAG_NVME_INITIATOR;
                        update = true;
                    }
                    if (update)
                    {
                        tx.put(rscKeyAndFlag.a, Long.toString(flag));
                    }
                }
            }
        }
    }

    private static class Pair<A, B>
    {
        public A a;
        public B b;

        Pair(A aRef, B bRef)
        {
            a = aRef;
            b = bRef;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((a == null) ? 0 : a.hashCode());
            result = prime * result + ((b == null) ? 0 : b.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Pair other = (Pair) obj;
            if (a == null)
            {
                if (other.a != null)
                    return false;
            }
            else if (!a.equals(other.a))
                return false;
            if (b == null)
            {
                if (other.b != null)
                    return false;
            }
            else if (!b.equals(other.b))
                return false;
            return true;
        }
    }

    private static class LayerRscHolder
    {
        public String nodeName;
        public String rscName;
        public String kind;

        LayerRscHolder()
        {
        }

        public boolean isComplete()
        {
            return nodeName != null && rscName != null && kind != null;
        }
    }
}
