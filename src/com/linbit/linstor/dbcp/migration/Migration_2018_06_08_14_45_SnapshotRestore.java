package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.dbdrivers.GenericDbDriver;

import java.sql.Connection;

@Migration(
    version = "2018.06.08.14.45",
    description = "Add size to snapshot volume definition"
)
public class Migration_2018_06_08_14_45_SnapshotRestore extends LinstorMigration
{
    @Override
    public void migrate(Connection connection)
        throws Exception
    {
        if (!MigrationUtils.columnExists(connection, "SNAPSHOT_VOLUME_DEFINITIONS", "VLM_SIZE"))
        {
            String sql = MigrationUtils.loadResource("2018_06_08_14_45_snapshot-restore.sql");
            GenericDbDriver.runSql(connection, sql);
        }
    }
}
