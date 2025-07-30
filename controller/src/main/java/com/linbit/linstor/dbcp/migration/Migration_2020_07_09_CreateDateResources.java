package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2020.07.09.08.00",
    description = "Add create timestamp column to resources table"
)
public class Migration_2020_07_09_CreateDateResources extends LinstorMigration
{
    @Override
    public void migrate(Connection dbCon, DbProduct dbProduct) throws Exception
    {
        if (!MigrationUtils.columnExists(dbCon, "RESOURCES", "CREATE_TIMESTAMP"))
        {
            SQLUtils.runSql(
                dbCon, MigrationUtils.addColumn(
                    dbProduct,
                    "RESOURCES",
                    "CREATE_TIMESTAMP",
                    "TIMESTAMP",
                    true,
                    null,
                    null
                )
            );
        }
    }
}
