package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;
import java.sql.Connection;

/**
 * Adds SpaceTracking capability by creating the required database tables
 */
@Migration(
    version = "2020.09.30.16.15",
    description = "Create tables required for SpaceTracking"
)
public class Migration_2020_09_30_SpaceTracking extends LinstorMigration
{
    @Override
    public void migrate(final Connection dbConn, final DbProduct product)
        throws Exception
    {
        final String sqlScript = MigrationUtils.loadResource("2020_09_30_spacetracking.sql");
        SQLUtils.runSql(dbConn, MigrationUtils.replaceTypesByDialect(product, sqlScript));
    }
}
