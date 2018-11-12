package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import java.sql.Connection;

import com.linbit.linstor.dbdrivers.GenericDbDriver;
import java.sql.Statement;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2018.10.08.13.00",
    description = "Add FLAGS column to RESOURCE_CONNECTIONS"
)
public class Migration_2018_10_08_ResourceConn_Flags extends LinstorMigration
{
    @Override
    public void migrate(Connection connection)
        throws Exception
    {
        if (!MigrationUtils.columnExists(connection, "RESOURCE_CONNECTIONS", "FLAGS"))
        {
            String crtTmpTblStmt;
            DatabaseInfo.DbProduct database = MigrationUtils.getDatabaseInfo().getDbProduct(connection.getMetaData());
            if (database == DatabaseInfo.DbProduct.DB2 ||
                database == DatabaseInfo.DbProduct.DB2_I ||
                database == DatabaseInfo.DbProduct.DB2_Z)
            {
                crtTmpTblStmt = "CREATE TABLE RESOURCE_CONNECTIONS_TMP AS (SELECT * FROM RESOURCE_CONNECTIONS) " +
                    "WITH DATA";
            }
            else
            {
                crtTmpTblStmt = "CREATE TABLE RESOURCE_CONNECTIONS_TMP AS SELECT * FROM RESOURCE_CONNECTIONS";
            }
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(crtTmpTblStmt);
            stmt.close();
            String sql = MigrationUtils.loadResource("2018_10_08_resource_connection_flags.sql");
            GenericDbDriver.runSql(connection, sql);
        }
    }
}
