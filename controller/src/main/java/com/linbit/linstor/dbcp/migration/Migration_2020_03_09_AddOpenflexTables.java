package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2020.03.09.13.37",
    description = "Add openflex tables"
)
public class Migration_2020_03_09_AddOpenflexTables extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        if (!MigrationUtils.tableExists(connection, "LAYER_OPENFLEX_RESOURCE_DEFINITIONS"))
        {
            SQLUtils.runSql(
                connection,
                MigrationUtils.loadResource("2020_03_09_add-openflex-layer-data.sql")
            );
        }
    }
}
