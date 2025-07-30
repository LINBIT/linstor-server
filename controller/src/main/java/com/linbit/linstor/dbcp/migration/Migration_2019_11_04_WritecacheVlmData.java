package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.11.04.01.01",
    description = "Adds new table for Writecache volumes"
)
public class Migration_2019_11_04_WritecacheVlmData extends LinstorMigration
{
    @Override
    public void migrate(Connection dbCon, DbProduct dbProduct) throws Exception
    {
        if (!MigrationUtils.tableExists(dbCon, "LAYER_WRITECACHE_VOLUMES"))
        {
            SQLUtils.runSql(
                dbCon,
                MigrationUtils.loadResource(
                    "2019_11_04_add-layer-writecache-volumes.sql"
                )
            );
        }
    }
}
