package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.05.27.13.44",
    description = "Make NODE_ID column in table SNAPSHOTS nullable"
)
/*
 * Resources without DRBD will have no NODE_ID, which means creating a snapshot
 * from such resources will cause an exception
 */
public class Migration_2019_05_27_Snapshot_NodeId_Nullable extends LinstorMigration
{
    private static final String TBL_SNAPSHOT = "SNAPSHOTS";

    private static final String S_NODE_ID = "NODE_ID";

    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct) throws Exception
    {
        // I could not figure out how to check if this update
        // was already applied...

        SQLUtils.runSql(
            connection,
            MigrationUtils.dropColumnConstraintNotNull(
                dbProduct,
                TBL_SNAPSHOT,
                S_NODE_ID,
                "INTEGER DEFAULT 0"
            )
        );
    }
}
