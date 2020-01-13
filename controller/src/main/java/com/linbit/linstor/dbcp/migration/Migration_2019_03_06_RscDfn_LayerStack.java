package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

@Migration(
    version = "2019.03.06.09.10",
    description = "Add new column layer_stack to resource-definition and snapshots"
)
public class Migration_2019_03_06_RscDfn_LayerStack extends LinstorMigration
{
     @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct)
        throws Exception
    {
        if (!MigrationUtils.columnExists(connection, "RESOURCE_DEFINITION", "LAYER_STACK"))
        {
            List<String> sqlStatements = new ArrayList<>();

            // we first have to let the DB insert null values, afterwards we use update-statements to
            // set the "dynamic default" values and after that we add the NOT NULL constraint
            sqlStatements.add(
                MigrationUtils.addColumn(
                    dbProduct,
                    "RESOURCE_DEFINITIONS",
                    "LAYER_STACK",
                    "VARCHAR(1024)",
                    true,
                    null,
                    null
                )
            );
            sqlStatements.add(
                MigrationUtils.addColumn(
                    dbProduct,
                    "SNAPSHOTS",
                    "LAYER_STACK",
                    "VARCHAR(1024)",
                    true,
                    null,
                    null
                )
            );

            for (String sql : sqlStatements)
            {
                SQLUtils.runSql(connection, sql);
            }

            ObjectMapper objectMapper = new ObjectMapper();

            try (PreparedStatement prepStmt = connection.prepareStatement(
                " SELECT " +
                    " RD.RESOURCE_NAME, " +
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
                " FROM RESOURCE_DEFINITIONS AS RD"
            ))
            {
                try (ResultSet resultSet = prepStmt.executeQuery())
                {
                    try (PreparedStatement update = connection.prepareStatement(
                        "UPDATE RESOURCE_DEFINITIONS " +
                        " SET LAYER_STACK = ? " +
                        " WHERE RESOURCE_NAME = ?"
                        )
                    )
                    {
                        while (resultSet.next())
                        {
                            List<String> stack = new ArrayList<>();
                            boolean swordfish = resultSet.getInt("HAS_SWORDFISH") > 0;
                            boolean encrypted = resultSet.getInt("IS_ENCRYPTED") > 0;
                            if (swordfish)
                            {
                                if (encrypted)
                                {
                                    stack.add("LUKS");
                                }
                                stack.add("STORAGE");
                            }
                            else
                            {
                                stack.add("DRBD");
                                if (encrypted)
                                {
                                    stack.add("LUKS");
                                }
                                stack.add("STORAGE");
                            }

                            update.setString(1, objectMapper.writeValueAsString(stack));
                            update.setString(2, resultSet.getString("RESOURCE_NAME"));

                            update.executeUpdate();
                        }
                    }
                }
            }
            try (PreparedStatement prepStmt = connection.prepareStatement(
                " SELECT " +
                    " S.UUID, " +
                    " ("  +
                        " SELECT COUNT(*)" +
                        " FROM VOLUME_DEFINITIONS VD" +
                        " WHERE VD.RESOURCE_NAME = S.RESOURCE_NAME AND" +
                        " VD.VLM_FLAGS = 2" +
                    " ) IS_ENCRYPTED " +
                    " FROM SNAPSHOTS AS S"
                ))
            {
                try (ResultSet resultSet = prepStmt.executeQuery())
                {
                    try (PreparedStatement update = connection.prepareStatement(
                        "UPDATE SNAPSHOTS " +
                        " SET LAYER_STACK = ?" +
                        " WHERE UUID = ?"
                        )
                    )
                    {
                        while (resultSet.next())
                        {
                            List<String> stack = new ArrayList<>();
                            boolean encrypted = resultSet.getInt("IS_ENCRYPTED") > 0;
                            // swordfish does not allow snapshots. no need to check
                            stack.add("DRBD");
                            if (encrypted)
                            {
                                stack.add("LUKS");
                            }
                            stack.add("STORAGE");

                            update.setString(1, objectMapper.writeValueAsString(stack));
                            update.setString(2, resultSet.getString("UUID"));
                            update.executeUpdate();
                        }
                    }
                }
            }

            sqlStatements.clear();
            sqlStatements.add(
                MigrationUtils.addColumnConstraintNotNull(
                    dbProduct,
                    "RESOURCE_DEFINITIONS",
                    "LAYER_STACK",
                    "VARCHAR(1024)"
                )
            );
            sqlStatements.add(
                MigrationUtils.addColumnConstraintNotNull(
                    dbProduct,
                    "SNAPSHOTS",
                    "LAYER_STACK",
                    "VARCHAR(1024)"
                )
            );

            for (String sql : sqlStatements)
            {
                SQLUtils.runSql(connection, sql);
            }

        }
    }
}
