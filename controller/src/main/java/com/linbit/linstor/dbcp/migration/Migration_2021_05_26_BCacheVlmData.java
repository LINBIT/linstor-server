package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2021.05.26.09.00",
    description = "Adds new table for BCache volumes"
)
public class Migration_2021_05_26_BCacheVlmData extends LinstorMigration
{
    @Override
    public void migrate(Connection dbCon, DbProduct dbProduct) throws Exception
    {
        if (!MigrationUtils.tableExists(dbCon, "LAYER_BCACHE_VOLUMES"))
        {
            SQLUtils.runSql(
                dbCon,
                MigrationUtils.loadResource(
                    "2021_05_26_add-layer-bcache-volumes.sql"
                )
            );
        }
    }
}
