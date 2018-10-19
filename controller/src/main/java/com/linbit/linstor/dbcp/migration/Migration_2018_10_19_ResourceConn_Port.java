package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.dbdrivers.GenericDbDriver;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2018.10.19.10.47",
    description = "Add TCP_PORT column to RESOURCE_CONNECTIONS"
)
public class Migration_2018_10_19_ResourceConn_Port extends LinstorMigration
{
    @Override
    public void migrate(Connection connection)
        throws Exception
    {
        if (!MigrationUtils.columnExists(connection, "RESOURCE_CONNECTIONS", "TCP_PORT"))
        {
            String sql = MigrationUtils.loadResource("2018_10_19_resource_connection_port.sql");
            GenericDbDriver.runSql(connection, sql);
        }
    }
}
