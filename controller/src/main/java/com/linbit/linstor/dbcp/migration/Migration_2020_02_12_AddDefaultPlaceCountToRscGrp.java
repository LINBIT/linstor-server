package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2020.02.12.09.30", description = "Add default place count to resource groups"
)
public class Migration_2020_02_12_AddDefaultPlaceCountToRscGrp extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        String updateStmtStr =
            " UPDATE RESOURCE_GROUPS " +
            " SET REPLICA_COUNT = 2 " +
            " WHERE REPLICA_COUNT IS NULL";
        try (PreparedStatement stmt = connection.prepareStatement(updateStmtStr))
        {
            stmt.executeUpdate();
        }

        SQLUtils.runSql(
            connection,
            MigrationUtils.addColumnConstraintNotNull(
                dbProduct,
                "RESOURCE_GROUPS",
                "REPLICA_COUNT",
                "INT"
            )
        );
    }
}
