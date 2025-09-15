package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

@EtcdMigration(
    description = "Move TCP Port number allocation from RscDfn to Rsc",
    version = 69
)
public class Migration_42_MoveTcpPortsToNodes extends BaseEtcdMigration
{
    private static final String ETCD_VALUE_NULL = ":null";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        final HashMap<String, Integer> tcpPortMap = getTcpPortsFromRdAndDeleteOldEntries(tx, prefix);
        setPortsOnRscLevel(tx, prefix, tcpPortMap);

        updateRscConDrbdProxyPorts(tx, prefix);
    }

    private HashMap<String, Integer> getTcpPortsFromRdAndDeleteOldEntries(EtcdTransaction tx, final String prefix)
    {
        // technically we would need to use RscDfnKey from Migration_2025_05_27_MoveTcpPortsToNodes.
        // however, currently DrbdRscDfnData simply cannot have a non empty rscNameSuffix. Therefore
        // we skip here the cumbersome building of DrbdRscDfnData from multiple ETCD map-entries and just
        // fetch the RscDfnName as well as the TCP port
        final HashMap<String, Integer> tcpPortMap = new HashMap<>();

        final String prefixedDbTableStr = prefix + "LAYER_DRBD_RESOURCE_DEFINITIONS/";
        final int prefixedTblKeyLen = prefixedDbTableStr.length();

        Map<String, String> result = tx.get(prefixedDbTableStr);
        for (Entry<String, String> entry : result.entrySet())
        {
            final String key = entry.getKey();
            if (key.endsWith("TCP_PORT"))
            {
                String combinedPkAndColumn = entry.getKey().substring(prefixedTblKeyLen);
                String combinedPk = combinedPkAndColumn.substring(0, combinedPkAndColumn.lastIndexOf("/"));

                String[] pks = combinedPk.split(":");
                String rscName = pks[0];

                // should never be null, but ... just to be sure
                @Nullable String tcpPortStr = entry.getValue();
                if (tcpPortStr != null && !tcpPortStr.equals(ETCD_VALUE_NULL))
                {
                    tcpPortMap.put(rscName, Integer.parseInt(tcpPortStr));
                }

                // do not delete the entry. TCP_PORT from LAYER_DRBD_RD will still be used as a fallback/default
            }
        }
        return tcpPortMap;
    }

    private void setPortsOnRscLevel(EtcdTransaction tx, String prefix, HashMap<String, Integer> tcpPortMapRef)
    {
        final String prefixedDbTableStr = prefix + "LAYER_RESOURCE_IDS/";
        final int prefixedTblKeyLen = prefixedDbTableStr.length();

        HashMap<Integer, LriHolder> lriById = new HashMap<>();

        Map<String, String> result = tx.get(prefixedDbTableStr);
        for (Entry<String, String> entry : result.entrySet())
        {
            String combinedPkAndColumn = entry.getKey().substring(prefixedTblKeyLen);
            int idxOfLastSeparator = combinedPkAndColumn.lastIndexOf("/");
            String combinedPk = combinedPkAndColumn.substring(0, idxOfLastSeparator);
            String clmName = combinedPkAndColumn.substring(idxOfLastSeparator + 1);

            String[] pks = combinedPk.split(":");
            int lriId = Integer.parseInt(pks[0]);

            LriHolder lriHolder = lriById.computeIfAbsent(lriId, ignored -> new LriHolder());
            switch (clmName)
            {
                case "NODE_NAME":
                    lriHolder.nodeName = entry.getValue();
                    break;
                case "RESOURCE_NAME":
                    lriHolder.rscName = entry.getValue();
                    break;
                case "SNAPSHOT_NAME":
                    lriHolder.snapName = entry.getValue();
                    break;
                case "LAYER_RESOURCE_KIND":
                    lriHolder.kind = entry.getValue();
                    break;
                default: // ignored
            }
        }

        for (Entry<Integer, LriHolder> entry : lriById.entrySet())
        {
            LriHolder lriHolder = entry.getValue();
            if (lriHolder.kind.equals("DRBD"))
            {
                if (!lriHolder.snapName.isEmpty())
                {
                    tx.put(
                        prefix + "LAYER_DRBD_RESOURCES/" + entry.getKey() + "/TCP_PORT_LIST",
                        "[-1]"
                    );
                }
                else
                {
                    tx.put(
                        prefix + "LAYER_DRBD_RESOURCES/" + entry.getKey() + "/TCP_PORT_LIST",
                        "[" + tcpPortMapRef.get(lriHolder.rscName) + "]"
                    );
                }
            }
        }
    }

    private void updateRscConDrbdProxyPorts(EtcdTransaction tx, String prefix)
    {
        final String prefixedDbTableStr = prefix + "RESOURCE_CONNECTIONS/";
        final int prefixedTblKeyLen = prefixedDbTableStr.length();

        Map<String, String> result = tx.get(prefixedDbTableStr);
        for (Entry<String, String> entry : result.entrySet())
        {
            final String key = entry.getKey();
            if (key.endsWith("TCP_PORT"))
            {
                String combinedPkAndColumn = entry.getKey().substring(prefixedTblKeyLen);
                String combinedPk = combinedPkAndColumn.substring(0, combinedPkAndColumn.lastIndexOf("/"));

                String value = entry.getValue();
                tx.put(prefixedDbTableStr + combinedPk + "/TCP_PORT_SRC", value);
                tx.put(prefixedDbTableStr + combinedPk + "/TCP_PORT_DST", value);

                // delete the original prefix+"RESOURCE_CONNECTIONS/.../TCP_PORT" entry in favor of the two new
                // ".../TCP_PORT_SRC" and ".../TCP_PORT_DST" entries.
                tx.delete(key);
            }
        }
    }

    private class LriHolder
    {
        // these fields are technically not nullable, but these fields are distributed across multiple map-entries
        // which means that these fields are temporarily nullable...
        private @Nullable String nodeName;
        private @Nullable String rscName;
        private @Nullable String snapName;
        private @Nullable String kind;
        // we do not care about the other entries.
        private LriHolder()
        {
        }
    }
}
