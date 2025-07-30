package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2020.01.20.15.42", description = "Add FLAGS to VolumeGroups"
)
public class Migration_2020_01_20_AddVlmGrpFlags extends LinstorMigration
{
    @Override
    public void migrate(Connection connectionRef, DbProduct dbProductRef) throws Exception
    {
        SQLUtils.runSql(
            connectionRef,
            MigrationUtils.addColumn(
                dbProductRef,
                "VOLUME_GROUPS",
                "FLAGS",
                "BIGINT",
                false,
                "0",
                null
            )
        );
    }
}
