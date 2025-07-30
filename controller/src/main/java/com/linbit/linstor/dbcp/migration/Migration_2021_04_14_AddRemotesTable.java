package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@Migration(
    version = "2021.04.14.12.00",
    description = "Add Remotes table"
)
public class Migration_2021_04_14_AddRemotesTable extends LinstorMigration
{

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        SQLUtils.runSql(
            connection,
            MigrationUtils.replaceTypesByDialect(dbProduct, MigrationUtils.loadResource("2021_04_14_add-remotes.sql"))
        );
    }

}
