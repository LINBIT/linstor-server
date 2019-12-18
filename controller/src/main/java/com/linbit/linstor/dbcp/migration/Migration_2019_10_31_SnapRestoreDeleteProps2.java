package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.10.31.01.01", description = "Delete the remaining properties after a snapshot restore operation"
)
public class Migration_2019_10_31_SnapRestoreDeleteProps2 extends LinstorMigration
{
    @Override
    protected void migrate(Connection dbCon, DbProduct dbProduct) throws Exception
    {
        SQLUtils.runSql(
            dbCon,
            "DELETE FROM PROPS_CONTAINERS " +
                "WHERE " +
                "PROPS_INSTANCE LIKE '/volumes/%' AND " +
                "(PROP_KEY = 'RestoreFromResource' OR " +
                " PROP_KEY = 'RestoreFromSnapshot')"
        );
    }
}
