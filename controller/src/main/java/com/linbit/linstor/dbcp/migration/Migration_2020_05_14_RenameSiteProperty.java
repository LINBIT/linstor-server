package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2020.05.14.13.37",
    description = "Rename property 'DrbdProxy/Site' to 'Site'"
)
public class Migration_2020_05_14_RenameSiteProperty extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        SQLUtils.runSql(
            connection,
            "UPDATE PROPS_CONTAINERS " +
                " SET PROP_KEY = 'Site' " +
                " WHERE PROP_KEY = 'DrbdProxy/Site'"
        );
    }
}
