package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.dbdrivers.GenericDbDriver;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.03.06.14.00",
    description = "Add RESOURCE_EXTERNAL_NAME column to RESOURCE_DEFINITIONS"
)
public class Migration_2019_03_06_ResourceDfn_ExternalName extends LinstorMigration
{
    @Override
    public void migrate(Connection connection)
        throws Exception
    {
        if (!MigrationUtils.columnExists(connection, "RESOURCE_DEFINITIONS", "RESOURCE_EXTERNAL_NAME"))
        {
            GenericDbDriver.runSql(
                connection,
                MigrationUtils.addColumn(
                    MigrationUtils.getDatabaseInfo().getDbProduct(connection.getMetaData()),
                    "RESOURCE_DEFINITIONS",
                    "RESOURCE_EXTERNAL_NAME",
                    "BLOB",
                    true,
                    null
                )
            );
        }
    }
}
