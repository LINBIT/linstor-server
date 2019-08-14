package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@Migration(
    version = "2018.06.13.10.26",
    description = "Add flags to snapshot volume definition"
)
public class Migration_2018_06_13_10_26_SnapshotEncryption extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct)
        throws Exception
    {
        if (!MigrationUtils.columnExists(connection, "SNAPSHOT_VOLUME_DEFINITIONS", "SNAPSHOT_FLAGS"))
        {
            String sql = MigrationUtils.loadResource("2018_06_13_10_26_snapshot-encryption.sql");
            SQLUtils.runSql(connection, sql);
        }
    }
}
