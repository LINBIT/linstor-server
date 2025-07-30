package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@Migration(
    version = "2022.01.16.12.00",
    description = "Migrate to SpaceTracking V2"
)
public class Migration_2022_01_24_SpaceTrackingV2 extends LinstorMigration
{
    @Override
    public void migrate(Connection dbCon, DatabaseInfo.DbProduct dbProductRef) throws Exception
    {
        final String statements = MigrationUtils.loadResource("2022_01_16_SpaceTrackingV2.sql");
        SQLUtils.runSql(dbCon, MigrationUtils.replaceTypesByDialect(dbProductRef, statements));
    }
}
