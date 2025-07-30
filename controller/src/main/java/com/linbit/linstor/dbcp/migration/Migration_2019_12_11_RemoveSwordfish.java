package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.12.11.01.01", description = "Remove swordfish table"
)
public class Migration_2019_12_11_RemoveSwordfish extends LinstorMigration
{
    @Override
    public void migrate(Connection connectionRef, DbProduct dbProductRef) throws Exception
    {
        SQLUtils.runSql(connectionRef, "DROP TABLE LAYER_SWORDFISH_VOLUME_DEFINITIONS");
        // swordfish was not supported for a long time, and most likely already broken in many places
    }
}
