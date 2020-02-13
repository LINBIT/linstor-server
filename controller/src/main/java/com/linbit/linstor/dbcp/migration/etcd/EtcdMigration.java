package com.linbit.linstor.dbcp.migration.etcd;

import static com.ibm.etcd.client.KeyUtils.bs;

import com.linbit.linstor.dbcp.migration.UsedByMigration;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;

import static com.linbit.linstor.dbdrivers.etcd.EtcdUtils.PATH_DELIMITER;
import static com.linbit.linstor.dbdrivers.etcd.EtcdUtils.PK_DELIMITER;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.DeleteRangeRequest;
import com.ibm.etcd.api.RangeRequest;
import com.ibm.etcd.client.KeyUtils;

public abstract class EtcdMigration
{
    protected static final String LINSTOR_PREFIX_PRE_07 = "LINSTOR/";

    @UsedByMigration
    public static RangeRequest getReq(String key, boolean recursive)
    {
        ByteString bsKey = bs(key);
        RangeRequest.Builder requestBuilder = RangeRequest.newBuilder().setKey(bsKey);
        if (recursive)
        {
            requestBuilder = requestBuilder.setRangeEnd(KeyUtils.plusOne(bsKey));
        }
        return requestBuilder.build();
    }

    @UsedByMigration
    public static DeleteRangeRequest delReq(String key, boolean recursive)
    {
        ByteString bsKey = bs(key);
        DeleteRangeRequest.Builder delBuilder = DeleteRangeRequest.newBuilder().setKey(bsKey);
        if (recursive)
        {
            delBuilder = delBuilder.setRangeEnd(KeyUtils.plusOne(bsKey));
        }
        return delBuilder.build();
    }

    public static String buildTableKeyPre07(String tableName, String... primKeys)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(LINSTOR_PREFIX_PRE_07).append(tableName).append(PATH_DELIMITER);
        if (primKeys.length > 0)
        {
            for (String pk : primKeys)
            {
                if (pk != null)
                {
                    sb.append(pk);
                }
                sb.append(PK_DELIMITER);
            }
            sb.setLength(sb.length() - PK_DELIMITER.length()); // cut last PK_DELIMITER
            sb.append(PATH_DELIMITER);
        }
        return sb.toString();
    }

    protected static String buildColumnKeyPre07(Column col, String... primKeys)
    {
        return buildTableKeyPre07(col.getTable().getName(), primKeys) + col.getName();
    }

    protected static String buildColumnKeyPre07(String tableName, String colName, String...primKeys)
    {
        return buildTableKeyPre07(tableName, primKeys) + colName;
    }

    protected static String extractPrimaryKeyPre07(String key)
    {
        // key is something like
        // LINSTOR/$table/$composedPk/$column = $valueOfColumn
        int tableStartIdx = LINSTOR_PREFIX_PRE_07.length();
        int composedKeyStartIdx = key.indexOf(PATH_DELIMITER, tableStartIdx + 1);
        int composedKeyEndIdx = key.lastIndexOf(PATH_DELIMITER);

        return key.substring(composedKeyStartIdx + 1, composedKeyEndIdx);
    }

    @UsedByMigration
    public static String getColumnName(String etcdKeyRef)
    {
        return etcdKeyRef.substring(etcdKeyRef.lastIndexOf(PATH_DELIMITER) + 1);
    }
}
