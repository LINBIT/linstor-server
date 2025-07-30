package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@Migration(
    version = "2021.04.12.12.00",
    description = "Add Files table"
)
public class Migration_2021_04_12_AddFilesTable extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        SQLUtils.runSql(
            connection,
            MigrationUtils.replaceTypesByDialect(
                dbProduct,
                MigrationUtils.loadResource(
                    "2021_04_12_add-files.sql"
                )
            )
        );
    }
}
