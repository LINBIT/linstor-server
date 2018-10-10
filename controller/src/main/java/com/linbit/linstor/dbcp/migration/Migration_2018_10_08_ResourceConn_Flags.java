package com.linbit.linstor.dbcp.migration;

import java.sql.Connection;

import com.linbit.linstor.dbdrivers.GenericDbDriver;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2018.10.08.13.00",
    description = "Add FLAGS column to RESOURCE_CONNECTIONS"
)
public class Migration_2018_10_08_ResourceConn_Flags extends LinstorMigration
{
    @Override
    public void migrate(Connection connection)
        throws Exception
    {
        if (!MigrationUtils.columnExists(connection, "RESOURCE_CONNECTIONS", "FLAGS"))
        {
            String sql = MigrationUtils.loadResource("2018_10_08_resource_connection_flags.sql");
            GenericDbDriver.runSql(connection, sql);
        }
    }
}
