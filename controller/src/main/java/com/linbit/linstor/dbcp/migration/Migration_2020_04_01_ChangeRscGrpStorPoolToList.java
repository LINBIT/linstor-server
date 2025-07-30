package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2020.04.01.13.37",
    description = "Extend resource groups pool_name column"
)
public class Migration_2020_04_01_ChangeRscGrpStorPoolToList extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        SQLUtils.runSql(
            connection,
            MigrationUtils.alterColumnType(
                dbProduct,
                "RESOURCE_GROUPS",
                "POOL_NAME",
                "VARCHAR(4096)"
            )
        );
        SQLUtils.runSql(
            connection,
            "UPDATE RESOURCE_GROUPS " +
                " SET POOL_NAME = " + MigrationUtils.concat(dbProduct, "'[\"'", "POOL_NAME", "'\"]'") +
                " WHERE POOL_NAME IS NOT NULL"
        );

        SQLUtils.runSql(
            connection,
            MigrationUtils.addColumn(
                dbProduct,
                "RESOURCE_GROUPS",
                "NODE_NAME_LIST",
                "VARCHAR(4096)",
                true,
                "[]",
                "REPLICA_COUNT"
            )
        );
    }
}
