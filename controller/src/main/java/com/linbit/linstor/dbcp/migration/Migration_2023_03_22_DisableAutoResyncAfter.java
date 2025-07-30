package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;
import java.sql.ResultSet;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2023.03.22.10.00",
    description = "Disable the automatic resync-after feature"
)
public class Migration_2023_03_22_DisableAutoResyncAfter extends LinstorMigration
{
    private static final String TBL_PROPS_CONTAINERS = "PROPS_CONTAINERS";
    private final static String KEY_AUTO_RESYNC_AFTER = ApiConsts.NAMESPC_DRBD_OPTIONS + "/auto-resync-after-disable";

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT 1 FROM " + TBL_PROPS_CONTAINERS + " WHERE PROPS_INSTANCE='/CTRLCFG' AND PROP_KEY='" +
                KEY_AUTO_RESYNC_AFTER + "'"
        );

        if (!rs.next())
        {
            SQLUtils.runSql(
                connection,
                "INSERT INTO " + TBL_PROPS_CONTAINERS + " (PROPS_INSTANCE, PROP_KEY, PROP_VALUE) " +
                    "VALUES ('/CTRLCFG', '" + KEY_AUTO_RESYNC_AFTER + "', 'true')"
            );
        }

        rs.close();
    }
}
