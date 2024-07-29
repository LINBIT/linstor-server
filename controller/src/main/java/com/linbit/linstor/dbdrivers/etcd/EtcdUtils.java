package com.linbit.linstor.dbdrivers.etcd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbcp.migration.UsedByMigration;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.client.kv.KvClient;
import io.grpc.Deadline;

import static com.ibm.etcd.client.KeyUtils.bs;

public class EtcdUtils
{
    @UsedByMigration
    public static final String PATH_DELIMITER = "/";
    @UsedByMigration
    public static final String PK_DELIMITER = ":";
    // not really a constant anymore as it can be set via a configuration property
    public static String linstorPrefix = null;

    private static final Deadline DEFAULT_DEADLINE = Deadline.after(60, TimeUnit.SECONDS);

    public static PutRequest putReq(String key, String value)
    {
        return PutRequest.newBuilder().setKey(bs(key)).setValue(bs(value)).build();
    }

    public static String buildKey(
        String tableName,
        String... pks
    )
    {
        return buildKey(tableName, true, pks);
    }

    public static String buildKey(
        String tableName,
        boolean includeTrailingPathDelimiterRef,
        String... pks
    )
    {
        StringBuilder sb = new StringBuilder();
        sb.append(linstorPrefix).append(tableName).append(PATH_DELIMITER);
        if (pks.length > 0)
        {
            for (String pk : pks)
            {
                if (pk != null)
                {
                    sb.append(pk);
                }
                sb.append(PK_DELIMITER);
            }
            sb.setLength(sb.length() - PK_DELIMITER.length()); // cut last PK_DELIMITER
            if (includeTrailingPathDelimiterRef)
            {
                sb.append(PATH_DELIMITER);
            }
        }
        return sb.toString();
    }

    public static String buildKey(
        DatabaseTable table,
        String... pks
    )
    {
        return buildKey(table.getName(), true, pks);
    }

    public static String buildKey(
        DatabaseTable table,
        boolean includeTrailingPathDelimiterRef,
        String... pks
    )
    {
        return buildKey(table.getName(), includeTrailingPathDelimiterRef, pks);
    }

    public static String buildKeyStr(
        String tableName,
        String columnName,
        String... pks
    )
    {
        return buildKey(tableName, pks) + columnName;
    }

    public static String buildKey(
        Column column,
        String... pks
    )
    {
        return buildKey(column.getTable(), pks) + column.getName();
    }

    public static String buildKey(
        String arbitraryKey,
        DatabaseTable table,
        String... pks
    )
    {
        return buildKey(table, pks) + arbitraryKey;
    }

    public static Map<String, String> getTableRow(KvClient client, String key)
    {
        RangeResponse rspRow = EtcdTransaction.requestWithRetry(client.get(bs(key)).asPrefix());

        HashMap<String, String> rowMap = new HashMap<>();
        for (KeyValue keyValue : rspRow.getKvsList())
        {
            // final String recKey = keyValue.getKey().toStringUtf8();
            // final String columnName = recKey.substring(recKey.lastIndexOf("/") + 1);
            rowMap.put(keyValue.getKey().toStringUtf8(), keyValue.getValue().toStringUtf8());
        }

        return rowMap;
    }

    public static String getFirstValue(KvClient kvClientRef, String primaryKey)
    {
        return getFirstValue(kvClientRef, primaryKey, null);
    }

    public static @Nullable String getFirstValue(KvClient kvClientRef, String primaryKeyRef, @Nullable String dfltValue)
    {
        Map<String, String> row = getTableRow(kvClientRef, primaryKeyRef);
        Iterator<String> iterator = row.values().iterator();
        String ret = dfltValue;
        if (iterator.hasNext())
        {
            ret = iterator.next();
        }
        return ret;
    }

    public static String getTablePk(Column col, String... pks)
    {
        StringBuilder ret = new StringBuilder();
        ret.append(linstorPrefix).append(col.getTable().getName()).append(PATH_DELIMITER);
        if (pks != null)
        {
            for (String pk : pks)
            {
                ret.append(pk).append(PK_DELIMITER);
            }
            if (pks.length > 0)
            {
                ret.setLength(ret.length() - PK_DELIMITER.length());
            }
        }
        return ret.toString();
    }

    public static Set<String> getComposedPkList(Map<String, String> tableRowRef)
    {
        Set<String> ret = new TreeSet<>();
        for (String key : tableRowRef.keySet())
        {
            String extractedPk = extractPrimaryKey(key);
            if (extractedPk != null)
            {
                ret.add(extractedPk);
            }
        }
        return ret;
    }

    /**
     * Returns the composed primary key, or null if there is no primary key. Note, an ETCD key like
     * "/LINSTOR/$table//$column" can still return an empty string as PK.
     * Only ETCD keys like "/LINSTOR/$table/$column" will return null (note "//" vs "/" between the table and column)
     */
    public static @Nullable String extractPrimaryKey(String key)
    {
        // key is something like
        // /LINSTOR/$table/$composedPk/$column = $valueOfColumn
        int tableStartIdx = linstorPrefix.length();
        int composedKeyStartIdx = key.indexOf(PATH_DELIMITER, tableStartIdx + 1);
        int composedKeyEndIdx = key.lastIndexOf(PATH_DELIMITER);

        String ret;
        if (composedKeyStartIdx == composedKeyEndIdx)
        {
            ret = null;
        }
        else
        {
            ret = key.substring(composedKeyStartIdx + 1, composedKeyEndIdx);
        }
        return ret;
    }

    public static String[] splitPks(@Nullable String composedPkRef, boolean emptyStringAsNull)
    {
        List<String> pks = new ArrayList<>();
        if (composedPkRef != null)
        {
            int startIdx = 0;
            int endIdx;
            // DO NOT USE String.split(":") as it will NOT include null pks
            do
            {
                endIdx = composedPkRef.indexOf(PK_DELIMITER, startIdx);
                if (endIdx != -1)
                {
                    if (startIdx == endIdx)
                    {
                        if (emptyStringAsNull)
                        {
                            pks.add(null);
                        }
                        else
                        {
                            pks.add("");
                        }
                    }
                    else
                    {
                        pks.add(composedPkRef.substring(startIdx, endIdx));
                    }
                    startIdx = endIdx + PK_DELIMITER.length();
                }
            }
            while (endIdx != -1);
            pks.add(composedPkRef.substring(startIdx)); // add pk after last ":"
        }

        String[] pkArr = new String[pks.size()];
        pks.toArray(pkArr);
        return pkArr;
    }

    private EtcdUtils()
    {
    }
}
