package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@Migration(
    version = "2022.01.24.14.00",
    description = "Add Schedules table"
)
public class Migration_2022_01_24_AddSchedulesTable extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        SQLUtils.runSql(
            connection,
            MigrationUtils.replaceTypesByDialect(dbProduct, MigrationUtils.loadResource("2022_01_24_add-schedules.sql"))
        );
    }
}
