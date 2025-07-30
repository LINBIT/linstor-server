package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2020.06.03.10.37",
    description = "Move all storage pool name properties into a simple single one"
)
public class Migration_2020_06_03_ConsolidatePoolProperty extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        SQLUtils.runSql(
            connection,
            "UPDATE PROPS_CONTAINERS SET PROP_KEY='StorDriver/StorPoolName'" +
                " WHERE PROPS_INSTANCE LIKE '/STORPOOLCONF%' AND" +
                " PROP_KEY IN ('StorDriver/LvmVg', 'StorDriver/ZPool', 'StorDriver/FileDir'," +
                " 'StorDriver/Openflex/StorPool', 'StorDriver/ZPoolThin')"
        );

        try (Statement stmt = connection.createStatement();
            ResultSet rsSet = stmt.executeQuery(
                "SELECT PROPS_INSTANCE, PROP_KEY, PROP_VALUE FROM PROPS_CONTAINERS" +
                    " WHERE PROPS_INSTANCE LIKE '/STORPOOLCONF%' AND" +
                    " PROP_KEY='StorDriver/ThinPool'"))
        {
            while (rsSet.next())
            {
                try (PreparedStatement updStmt = connection.prepareStatement(
                    "UPDATE PROPS_CONTAINERS" +
                        " SET PROP_VALUE=CONCAT(PROP_VALUE,'/',?)" +
                        " WHERE PROPS_INSTANCE=? AND PROP_KEY='StorDriver/StorPoolName'"))
                {
                    updStmt.setString(1, rsSet.getString("PROP_VALUE"));
                    updStmt.setString(2, rsSet.getString("PROPS_INSTANCE"));
                    updStmt.executeUpdate();
                }
            }
        }

        SQLUtils.runSql(connection, "DELETE FROM PROPS_CONTAINERS" +
            " WHERE PROP_KEY='StorDriver/ThinPool'");
    }
}
