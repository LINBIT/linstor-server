package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.GenericDbDriver;
import java.sql.Connection;
import java.sql.Statement;

@SuppressWarnings({"checkstyle:typename", "checkstyle:magicnumber"})
@Migration(
    version = "2019.03.06.09.10",
    description = "Add new column for (optional) layer stack to resource-definition and snapshots"
)
public class Migration_2019_03_06_RscDfn_LayerStack extends LinstorMigration
{
     @Override
    public void migrate(Connection connection)
        throws Exception
    {
        if (!MigrationUtils.columnExists(connection, "RESOURCE_DEFINITION", "LAYER_KIND_STACK"))
        {
            String crtTmpRscDfnTblStmt;
            String crtTmpSnapTblStmt;
            DatabaseInfo.DbProduct database = MigrationUtils.getDatabaseInfo().getDbProduct(connection.getMetaData());
            if (database == DatabaseInfo.DbProduct.DB2 ||
                database == DatabaseInfo.DbProduct.DB2_I ||
                database == DatabaseInfo.DbProduct.DB2_Z)
            {
                crtTmpRscDfnTblStmt = "CREATE TABLE RESOURCE_DEFINITIONS_TMP AS (SELECT * FROM RESOURCE_DEFINITIONS) " +
                    "WITH DATA";
                crtTmpSnapTblStmt = "CREATE TABLE SNAPSHOTS_TMP AS (SELECT * FROM SNAPSHOTS) " +
                    "WITH DATA";
            }
            else
            {
                crtTmpRscDfnTblStmt = "CREATE TABLE RESOURCE_DEFINITIONS_TMP AS SELECT * FROM RESOURCE_DEFINITIONS";
                crtTmpSnapTblStmt = "CREATE TABLE SNAPSHOTS_TMP AS SELECT * FROM SNAPSHOTS";
            }
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(crtTmpRscDfnTblStmt);
            stmt.executeUpdate(crtTmpSnapTblStmt);
            stmt.close();
            String sql = MigrationUtils.loadResource("2019_03_06_rscdfn_layerstack.sql");
            GenericDbDriver.runSql(connection, sql);
        }
    }
}
