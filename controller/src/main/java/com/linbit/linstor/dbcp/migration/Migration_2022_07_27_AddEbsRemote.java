package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@Migration(
    version = "2022.07.27.09.00",
    description = "Add EBS_Remote table"
)
public class Migration_2022_07_27_AddEbsRemote extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        SQLUtils.runSql(
            connection,
            MigrationUtils.replaceTypesByDialect(
                dbProduct,
                MigrationUtils.loadResource("2022_07_27_add-ebs-remotes.sql")
            )
        );
    }
}
