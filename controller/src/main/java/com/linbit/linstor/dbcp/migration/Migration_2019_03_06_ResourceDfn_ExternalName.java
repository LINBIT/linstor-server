package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.03.06.14.00",
    description = "Add RESOURCE_EXTERNAL_NAME column to RESOURCE_DEFINITIONS"
)
public class Migration_2019_03_06_ResourceDfn_ExternalName extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct)
        throws Exception
    {
        if (!MigrationUtils.columnExists(connection, "RESOURCE_DEFINITIONS", "RESOURCE_EXTERNAL_NAME"))
        {
            SQLUtils.runSql(
                connection,
                MigrationUtils.addColumn(
                    dbProduct,
                    "RESOURCE_DEFINITIONS",
                    "RESOURCE_EXTERNAL_NAME",
                    "BLOB",
                    true,
                    null,
                    null
                )
            );
        }
    }
}
