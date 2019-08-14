package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.01.17.10.48",
    description = "Change controller props instance name"
)
public class Migration_2019_01_17_KeyValueStore extends LinstorMigration
{
    private static final String TBL_PROPS_CONTAINERS = "PROPS_CONTAINERS";

    private static final String S_PROPS_INSTANCE = "PROPS_INSTANCE";
    private static final String OLD_INSTANCE_NAME = "CTRLCFG";
    private static final String NEW_INSTANCE_NAME = "/CTRLCFG";

    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct)
        throws Exception
    {
        if (!MigrationUtils.tableExists(connection, "KEY_VALUE_STORE"))
        {
            // unify instance name prefixes
            SQLUtils.runSql(
                connection,
                "UPDATE " + TBL_PROPS_CONTAINERS +
                " SET " + S_PROPS_INSTANCE + " = '" + NEW_INSTANCE_NAME +
                "' WHERE " + S_PROPS_INSTANCE + " = '" + OLD_INSTANCE_NAME + "';"
            );

            SQLUtils.runSql(
                connection,
                MigrationUtils.loadResource(
                    "2019_01_21_add-kvs.sql"
                )
            );
        }
    }
}
