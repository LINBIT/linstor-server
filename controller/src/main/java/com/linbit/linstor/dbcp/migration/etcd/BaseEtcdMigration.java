package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.transaction.EtcdTransaction;

import static com.linbit.linstor.dbdrivers.etcd.EtcdUtils.PATH_DELIMITER;
import static com.linbit.linstor.dbdrivers.etcd.EtcdUtils.PK_DELIMITER;

public abstract class BaseEtcdMigration
{
    protected static final String LINSTOR_PREFIX_PRE_34 = "LINSTOR/";
    private final int version;
    private final String description;

    public BaseEtcdMigration()
    {
        EtcdMigration etcdMigAnnot = this.getClass().getAnnotation(EtcdMigration.class);
        version = etcdMigAnnot.version();
        description = etcdMigAnnot.description();
    }

    public int getVersion()
    {
        return version;
    }

    public int getNextVersion()
    {
        return version + 1;
    }

    public String getDescription()
    {
        return description;
    }

    protected String buildTableKey(String tableName, String... primKeys)
    {
        String key;
        if (version <= 34) // 34 changes "LINSTOR/" to "/LINSTOR/"
        {
            key = buildTableKeyPre34(tableName, primKeys);
        }
        else
        {
            key = EtcdUtils.buildKey(tableName, primKeys);
        }
        return key;
    }

    public String buildColumnKey(Column col, String... primKeys)
    {
        String key;
        if (version <= 34) // 34 changes "LINSTOR/" to "/LINSTOR/"
        {
            key = buildTableKeyPre34(col.getTable().getName(), primKeys) + col.getName();
        }
        else
        {
            key = EtcdUtils.buildKey(col, primKeys);
        }
        return key;
    }

    public String buildKeyStr(
        String tableName,
        String columnName,
        String... pks
    )
    {
        String key;
        if (version <= 34) // 34 changes "LINSTOR/" to "/LINSTOR/"
        {
            key = buildTableKey(tableName, pks) + columnName;
        }
        else
        {
            key = EtcdUtils.buildKeyStr(tableName, columnName, pks);
        }
        return key;
    }

    public String extractPrimaryKey(String fullEtcdKey)
    {
        String primaryKey;
        if (version <= 34) // 34 changes "LINSTOR/" to "/LINSTOR/"
        {
            primaryKey = extractPrimaryKeyPre34(fullEtcdKey);
        }
        else
        {
            primaryKey = EtcdUtils.extractPrimaryKey(fullEtcdKey);
        }
        return primaryKey;
    }

    private String buildTableKeyPre34(String tableName, String... primKeys)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(LINSTOR_PREFIX_PRE_34).append(tableName).append(PATH_DELIMITER);
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

    private String extractPrimaryKeyPre34(String key)
    {
        // key is something like
        // LINSTOR/$table/$composedPk/$column = $valueOfColumn
        int tableStartIdx = LINSTOR_PREFIX_PRE_34.length();
        int composedKeyStartIdx = key.indexOf(PATH_DELIMITER, tableStartIdx + 1);
        int composedKeyEndIdx = key.lastIndexOf(PATH_DELIMITER);

        return key.substring(composedKeyStartIdx + 1, composedKeyEndIdx);
    }

    protected String getColumnName(String etcdKeyRef)
    {
        return etcdKeyRef.substring(etcdKeyRef.lastIndexOf(PATH_DELIMITER) + 1);
    }

    public abstract void migrate(EtcdTransaction tx) throws Exception;
}
