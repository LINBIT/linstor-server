package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

/*
 * ETCD migration not needed.
 * StorPoolDbDriver restores the value using Boolean.parseBoolean, which return false if the parameter is null.
 */
@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2020.10.13.12.40",
    description = "Add EXTERNAL_LOCKING column with default value false"
)
public class Migration_2020_10_13_StorPool_Add_ExternalLocking extends LinstorMigration
{
    private static final String TBL_STOR_POOL = "NODE_STOR_POOL";
    private static final String NEW_CLM = "EXTERNAL_LOCKING";
    private static final boolean DFLT_VALUE = false;

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        if (!MigrationUtils.columnExists(connection, TBL_STOR_POOL, NEW_CLM))
        {
            SQLUtils.runSql(
                connection,
                MigrationUtils.addColumn(
                    dbProduct,
                    TBL_STOR_POOL,
                    NEW_CLM,
                    "BOOL",
                    false,
                    Boolean.toString(DFLT_VALUE),
                    null
                )
            );
        }
    }
}
