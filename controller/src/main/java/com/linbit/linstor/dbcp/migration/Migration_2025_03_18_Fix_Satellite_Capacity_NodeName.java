package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2025.03.18.08.00",
    description = "Fix SATELLITES_CAPACITY NODE_NAME column being too short"
)
public class Migration_2025_03_18_Fix_Satellite_Capacity_NodeName extends LinstorMigration
{
    @Override
    protected void migrate(Connection conRef, DbProduct dbProduct) throws Exception
    {
        SQLUtils.runSql(
            conRef,
            MigrationUtils.alterColumnType(
                dbProduct,
                "SATELLITES_CAPACITY",
                "NODE_NAME",
                "VARCHAR(255)"
            )
        );
    }
}
