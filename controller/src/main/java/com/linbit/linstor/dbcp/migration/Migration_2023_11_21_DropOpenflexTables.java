package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2023.11.21.13.37",
    description = "Drop openflex tables"
)
public class Migration_2023_11_21_DropOpenflexTables extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        if (MigrationUtils.tableExists(connection, "LAYER_OPENFLEX_RESOURCE_DEFINITIONS"))
        {
            SQLUtils.runSql(connection, MigrationUtils.dropTable(dbProduct, "LAYER_OPENFLEX_RESOURCE_DEFINITIONS"));
        }
        if (MigrationUtils.tableExists(connection, "LAYER_OPENFLEX_VOLUMES"))
        {
            SQLUtils.runSql(connection, MigrationUtils.dropTable(dbProduct, "LAYER_OPENFLEX_VOLUMES"));
        }
    }
}
