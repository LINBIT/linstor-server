package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2026.03.09.00.00",
    description = "Add alternative suffixes column to Files table"
)
public class Migration_2026_03_09_AddFilesAltSuffixes extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        SQLUtils.executeStatement(
            connection,
            MigrationUtils.addColumn(
                dbProduct,
                "FILES",
                "ALT_SUFFIXES",
                "BLOB",
                false,
                "[]",
                null
            )
        );
    }
}
