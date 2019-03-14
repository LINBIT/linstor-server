package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.GenericDbDriver;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

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
        if (!MigrationUtils.columnExists(connection, "RESOURCE_DEFINITION", "LAYER_STACK"))
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

            ObjectMapper objectMapper = new ObjectMapper();

            try (PreparedStatement prepStmt = connection.prepareStatement(
                " SELECT " +
                    " RD.UUID, RD.RESOURCE_NAME, RD.RESOURCE_DSP_NAME, RD.RESOURCE_FLAGS, " +
                    " ("  +
                        " SELECT COUNT(*)" +
                        " FROM VOLUME_DEFINITIONS VD" +
                        " WHERE VD.RESOURCE_NAME = RD.RESOURCE_NAME AND" +
                        " VD.VLM_FLAGS = 2" +
                    " ) IS_ENCRYPTED, " +
                    " ( " +
                        " SELECT COUNT(*)" +
                        " FROM NODE_STOR_POOL NSP, VOLUMES V" +
                        " WHERE NSP.POOL_NAME = V.STOR_POOL_NAME AND" +
                              " NSP.NODE_NAME = V.NODE_NAME AND" +
                              " V.RESOURCE_NAME = RD.RESOURCE_NAME AND" +
                              " NSP.DRIVER_NAME LIKE 'Swordfish%'" +
                    " ) HAS_SWORDFISH" +
                " FROM RESOURCE_DEFINITIONS_TMP AS RD"
            ))
            {
                try (ResultSet resultSet = prepStmt.executeQuery())
                {
                    try (PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO RESOURCE_DEFINITIONS " +
                        " (UUID, RESOURCE_NAME, RESOURCE_DSP_NAME, RESOURCE_FLAGS, LAYER_STACK)" +
                        " VALUES (?, ?, ?, ?, ?)"
                        )
                    )
                    {
                        while (resultSet.next())
                        {
                            insert.setString(1, resultSet.getString("UUID"));
                            insert.setString(2, resultSet.getString("RESOURCE_NAME"));
                            insert.setString(3, resultSet.getString("RESOURCE_DSP_NAME"));
                            insert.setLong(4, resultSet.getLong("RESOURCE_FLAGS"));

                            List<String> stack = new ArrayList<>();
                            boolean swordfish = resultSet.getInt("HAS_SWORDFISH") > 0;
                            boolean encrypted = resultSet.getInt("IS_ENCRYPTED") > 0;
                            if (swordfish)
                            {
                                if (encrypted)
                                {
                                    stack.add("CRYPT_SETUP");
                                }
                                stack.add("STORAGE");
                            }
                            else
                            {
                                stack.add("DRBD");
                                if (encrypted)
                                {
                                    stack.add("ENCRYPTED");
                                }
                                stack.add("STORAGE");
                            }

                            insert.setString(5, objectMapper.writeValueAsString(stack));

                            insert.executeUpdate();
                        }
                    }
                }
            }
            try (PreparedStatement prepStmt = connection.prepareStatement(
                " SELECT " +
                    " S.UUID, S.NODE_NAME, S.RESOURCE_NAME, S.SNAPSHOT_NAME, S.SNAPSHOT_FLAGS, S.NODE_ID, " +
                    " ("  +
                        " SELECT COUNT(*)" +
                        " FROM VOLUME_DEFINITIONS VD" +
                        " WHERE VD.RESOURCE_NAME = S.RESOURCE_NAME AND" +
                        " VD.VLM_FLAGS = 2" +
                    " ) IS_ENCRYPTED " +
                    " FROM SNAPSHOTS_TMP AS S"
                ))
            {
                try (ResultSet resultSet = prepStmt.executeQuery())
                {
                    try (PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO SNAPSHOTS " +
                            " (UUID, NODE_NAME, RESOURCE_NAME, SNAPSHOT_NAME, SNAPSHOT_FLAGS, NODE_ID, LAYER_STACK)" +
                            " VALUES (?, ?, ?, ?, ?, ?, ?)"
                        )
                        )
                    {
                        while (resultSet.next())
                        {
                            insert.setString(1, resultSet.getString("UUID"));
                            insert.setString(2, resultSet.getString("NODE_NAME"));
                            insert.setString(3, resultSet.getString("RESOURCE_NAME"));
                            insert.setString(4, resultSet.getString("SNAPSHOT_NAME"));
                            insert.setLong(5, resultSet.getLong("SNAPSHOT_FLAGS"));
                            insert.setInt(6, resultSet.getInt("NODE_ID"));

                            List<String> stack = new ArrayList<>();
                            boolean encrypted = resultSet.getInt("IS_ENCRYPTED") > 0;
                            // swordfish does not allow snapshots. no need to check
                            stack.add("DRBD");
                            if (encrypted)
                            {
                                stack.add("ENCRYPTED");
                            }
                            stack.add("STORAGE");

                            insert.setString(7, objectMapper.writeValueAsString(stack));
                            insert.executeUpdate();
                        }
                    }
                }
            }

            GenericDbDriver.runSql(connection, "DROP TABLE RESOURCE_DEFINITIONS_TMP;");
            GenericDbDriver.runSql(connection, "DROP TABLE SNAPSHOTS_TMP;");
        }
    }
}
