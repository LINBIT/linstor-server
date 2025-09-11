package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@Migration(
    version = "2026.01.30.09.00",
    description = "Add AuthTokens table"
)
public class Migration_2026_01_30_AddAuthTokensTable extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        SQLUtils.runSql(
            connection,
            MigrationUtils.replaceTypesByDialect(
                dbProduct,
                MigrationUtils.loadResource("2026_01_30_add-auth-tokens.sql")
            )
        );
    }
}
