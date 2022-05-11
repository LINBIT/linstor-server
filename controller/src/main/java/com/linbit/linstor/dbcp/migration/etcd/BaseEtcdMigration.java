package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbcp.migration.AbsMigration;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.transaction.EtcdTransaction;

import static com.linbit.linstor.dbdrivers.etcd.EtcdUtils.PATH_DELIMITER;

public abstract class BaseEtcdMigration extends AbsMigration
{
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
        return EtcdUtils.buildKey(tableName, primKeys);
    }

    public String buildColumnKey(Column col, String... primKeys)
    {
        return EtcdUtils.buildKey(col, primKeys);
    }

    public String buildKeyStr(
        String tableName,
        String columnName,
        String... pks
    )
    {
        return EtcdUtils.buildKeyStr(tableName, columnName, pks);
    }

    public String extractPrimaryKey(String fullEtcdKey)
    {
        return EtcdUtils.extractPrimaryKey(fullEtcdKey);
    }

    protected String getColumnName(String etcdKeyRef)
    {
        return etcdKeyRef.substring(etcdKeyRef.lastIndexOf(PATH_DELIMITER) + 1);
    }

    public abstract void migrate(EtcdTransaction tx, String prefix) throws Exception;
}
