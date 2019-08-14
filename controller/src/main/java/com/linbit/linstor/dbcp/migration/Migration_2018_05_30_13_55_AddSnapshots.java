package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@Migration(
    version = "2018.05.30.15.55",
    description = "Add snapshots and snapshot volume definitions"
)
public class Migration_2018_05_30_13_55_AddSnapshots extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct)
        throws Exception
    {
        if (!MigrationUtils.tableExists(connection, "SNAPSHOTS"))
        {
            String sql = MigrationUtils.loadResource("2018_05_30_15_55_add-snapshots.sql");
            SQLUtils.runSql(connection, sql);
        }
    }
}
