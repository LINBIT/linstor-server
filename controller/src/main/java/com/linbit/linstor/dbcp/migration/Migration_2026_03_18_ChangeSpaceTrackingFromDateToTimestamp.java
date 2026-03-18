package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2026.03.18.09.00",
    description = "Change SpaceTracking tables from using DATE to TIMESTAMP"
)
public class Migration_2026_03_18_ChangeSpaceTrackingFromDateToTimestamp extends LinstorMigration
{

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        connection.createStatement()
            .execute(
                MigrationUtils.alterColumnType(dbProduct, "SPACE_HISTORY", "ENTRY_DATE", "TIMESTAMP")
            );
        connection.createStatement()
            .execute(
                MigrationUtils.alterColumnType(dbProduct, "TRACKING_DATE", "ENTRY_DATE", "TIMESTAMP")
            );
    }
}
