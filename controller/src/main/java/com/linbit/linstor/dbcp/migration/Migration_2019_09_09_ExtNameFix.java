package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;


import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.09.09.01.01",
    description = "Fix resource definition external name entries"
)
/**
 * Fixes the resource definition external name entries
 */
public class Migration_2019_09_09_ExtNameFix extends LinstorMigration
{
    @Override
    public void migrate(Connection dbCon, DbProduct dbProduct) throws Exception
    {
        // Transform zero-length external names into NULL entries
        SQLUtils.runSql(dbCon, "UPDATE RESOURCE_DEFINITIONS " +
            "SET RESOURCE_EXTERNAL_NAME = NULL WHERE LENGTH(RESOURCE_EXTERNAL_NAME) = 0");
    }
}
