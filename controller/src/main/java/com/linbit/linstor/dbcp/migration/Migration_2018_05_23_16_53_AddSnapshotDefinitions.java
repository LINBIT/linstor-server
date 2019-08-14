package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@Migration(
    version = "2018.05.23.16.53",
    description = "Add snapshot definitions"
)
public class Migration_2018_05_23_16_53_AddSnapshotDefinitions extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct)
        throws Exception
    {
        if (!MigrationUtils.tableExists(connection, "SNAPSHOT_DEFINITIONS"))
        {
            String sql = MigrationUtils.loadResource("2018_05_23_16_53_add-snapshot-definitions.sql");
            SQLUtils.runSql(connection, sql);
        }
    }
}
