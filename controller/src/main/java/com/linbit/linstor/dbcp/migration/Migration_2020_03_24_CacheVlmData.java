package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2020.03.24.01.01",
    description = "Adds new table for Cache volumes"
)
public class Migration_2020_03_24_CacheVlmData extends LinstorMigration
{
    @Override
    public void migrate(Connection dbCon, DbProduct dbProduct) throws Exception
    {
        if (!MigrationUtils.tableExists(dbCon, "LAYER_CACHE_VOLUMES"))
        {
            SQLUtils.runSql(
                dbCon,
                MigrationUtils.loadResource(
                    "2020_03_24_add-layer-cache-volumes.sql"
                )
            );
        }
    }
}
