package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.12.03.01.01",
    description = "Add new column LAYER_RESOURCE_SUSPENDED to LAYER_RESOURCE_IDS"
)
public class Migration_2019_12_03_LayerResource_AddSuspendColumn extends LinstorMigration
{
    @Override
    public void migrate(Connection dbCon, DbProduct dbProduct) throws Exception
    {
        if (!MigrationUtils.columnExists(dbCon, "LAYER_RESOURCE_IDS", "LAYER_RESOURCE_SUSPENDED"))
        {
            SQLUtils.runSql(
                dbCon, MigrationUtils.addColumn(
                    dbProduct,
                    "LAYER_RESOURCE_IDS",
                    "LAYER_RESOURCE_SUSPENDED",
                    "BOOL",
                    false,
                    "FALSE",
                    null
                )
            );
        }
    }
}
