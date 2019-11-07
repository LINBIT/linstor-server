package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.transaction.ControllerETCDTransactionMgr;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.ibm.etcd.client.kv.KvClient.FluentTxnOps;

// corresponds to Migration_2019_11_12_DisklessFlagSplit
public class Migration_04_DisklessFlagSplit extends EtcdMigration
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
    public static void migrate(ControllerETCDTransactionMgr txMgr)
    {
        TreeMap<String, String> allRscLayer = txMgr.readTable("LINSTOR/LAYER_RESOURCE_IDS", true);

        // key: <nodeName, rscName>, value: <etcdKey, rscFlag>
        HashMap<Pair<String, String>, Pair<String, Long>> rscMap = new HashMap<>();
        {
            TreeMap<String, String> allRsc = txMgr.readTable("LINSTOR/RESOURCES", true);
            for (Entry<String, String> rsc : allRsc.entrySet())
            {
                String key = rsc.getKey();
                if (key.endsWith(RSC_FLAGS))
                {
                    String nodeName;
                    String rscName;
                    {
                        String composedKey = EtcdUtils.extractPrimaryKey(key);
                        String[] split = composedKey.split(EtcdUtils.PK_DELIMITER);
                        nodeName = split[0];
                        rscName = split[1];
                    }
                    long flags = Long.parseLong(rsc.getValue());

                    rscMap.put(new Pair<>(nodeName, rscName), new Pair<>(key, flags));
                }
            }
        }

        FluentTxnOps<?> tx = txMgr.getTransaction();

        HashMap<Long, LayerRscHolder> rscDataMap = new HashMap<>();
        for (Entry<String, String> rscLayer : allRscLayer.entrySet())
        {
            String etcdKey = rscLayer.getKey();
            long layerRscId = Long.parseLong(EtcdUtils.extractPrimaryKey(etcdKey));

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
                    else if (layerRscHolder.kind.equals(KIND_NVME))
                    {
                        flag |= FLAG_NVME_INITIATOR;
                        update = true;
                    }
                    if (update)
                    {
                        tx.put(putReq(rscKeyAndFlag.a, Long.toString(flag)));
                    }
                }
            }
        }
    }

    private static class Pair<A, B>
    {
        public A a;
        public B b;

        public Pair(A aRef, B bRef)
        {
            a = aRef;
            b = bRef;
        }
    }

    private static class LayerRscHolder
    {
        public String nodeName;
        public String rscName;
        public String kind;

        public LayerRscHolder()
        {
        }

        public boolean isComplete()
        {
            return nodeName != null && rscName != null && kind != null;
        }
    }
}
