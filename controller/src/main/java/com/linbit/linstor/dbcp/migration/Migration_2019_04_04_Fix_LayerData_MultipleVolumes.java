package com.linbit.linstor.dbcp.migration;

import com.linbit.ImplementationError;
import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.utils.Base64;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Migration(
    version = "2019.04.04.09.53",
    description = "Fixing incorrect Migration LayerData"
)
public class Migration_2019_04_04_Fix_LayerData_MultipleVolumes extends LinstorMigration
{
    private static final String SELECT_ALL_LAYER_RESOURCE_IDS =
        " SELECT LAYER_RESOURCE_ID, NODE_NAME, RESOURCE_NAME, LAYER_RESOURCE_KIND, " +
                "LAYER_RESOURCE_PARENT_ID, LAYER_RESOURCE_SUFFIX " +
        " FROM LAYER_RESOURCE_IDS";
    private static final String SELECT_ALL_LUKS_VLMS =
        " SELECT LUKS.LAYER_RESOURCE_ID, LUKS.VLM_NR, " +
                "LRI.RESOURCE_NAME " +
        " FROM LAYER_LUKS_VOLUMES AS LUKS, LAYER_RESOURCE_IDS AS LRI " +
        " WHERE LUKS.LAYER_RESOURCE_ID = LRI.LAYER_RESOURCE_ID";
    private static final String SELECT_SINGLE_PROP =
        "SELECT PROP_VALUE " +
        "FROM PROPS_CONTAINERS " +
        "WHERE PROPS_INSTANCE = ? AND " +
               "PROP_KEY = ?";

    private static final String DELETE_DUP_DRBD_RESOURCE =
        " DELETE FROM LAYER_DRBD_RESOURCES " +
        " WHERE LAYER_RESOURCE_ID = ?;";
    private static final String DELETE_DUP_LAYER_RESOURCE_ID =
        " DELETE FROM LAYER_RESOURCE_IDS " +
        " WHERE LAYER_RESOURCE_ID = ?;";

    private static final String UPDATE_DUP_LUKS_VOLUME =
        " UPDATE LAYER_LUKS_VOLUMES " +
        " SET LAYER_RESOURCE_ID = ? " +
        " WHERE LAYER_RESOURCE_ID = ?;";
    private static final String UPDATE_DUP_STORAGE_VOLUME =
        " UPDATE LAYER_STORAGE_VOLUMES " +
        " SET LAYER_RESOURCE_ID = ? " +
        " WHERE LAYER_RESOURCE_ID = ?;";

    private static final String UPDATE_LUKS_BASE64_DECODE =
        " UPDATE LAYER_LUKS_VOlUMES " +
        " SET ENCRYPTED_PASSWORD = ? " +
        " WHERE LAYER_RESOURCE_ID = ? AND " +
               "VLM_NR = ?;";

    @Override
    protected void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct) throws Exception
    {
        Map<Triple<String, String, String>, Integer> layerIds = new HashMap<>();

        try (PreparedStatement stmt = connection.prepareStatement(SELECT_ALL_LAYER_RESOURCE_IDS))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    Triple<String, String, String> key = new Triple<>(
                        resultSet.getString("NODE_NAME"),
                        resultSet.getString("RESOURCE_NAME"),
                        resultSet.getString("LAYER_RESOURCE_KIND")
                    );
                    int id = resultSet.getInt("LAYER_RESOURCE_ID");
                    Integer origId = layerIds.get(key);
                    if (origId == null)
                    {
                        layerIds.put(key, id);
                    }
                    else
                    {
                        int duplicateId = id;
                        update(connection, UPDATE_DUP_LUKS_VOLUME, origId, duplicateId);
                        update(connection, UPDATE_DUP_STORAGE_VOLUME, origId, duplicateId);
                        delete(connection, DELETE_DUP_DRBD_RESOURCE, duplicateId);
                        delete(connection, DELETE_DUP_LAYER_RESOURCE_ID, duplicateId);
                    }
                }
            }
        }

        try (PreparedStatement selectLuksVlms = connection.prepareStatement(SELECT_ALL_LUKS_VLMS))
        {
            try (ResultSet resultSet = selectLuksVlms.executeQuery())
            {
                try (PreparedStatement updateStmt = connection.prepareStatement(UPDATE_LUKS_BASE64_DECODE))
                {
                    try (PreparedStatement selectKey = connection.prepareStatement(SELECT_SINGLE_PROP))
                    {
                        while (resultSet.next())
                        {
                            String rscName = resultSet.getString("RESOURCE_NAME");
                            int vlmNr = resultSet.getInt("VLM_NR");
                            String encryptedPw;
                            selectKey.setString(1, "/VOLUMEDEFINITIONS/" + rscName.toUpperCase() + "/" + vlmNr);
                            selectKey.setString(2, "CryptPasswd");

                            try (ResultSet encryptedKeyResultSet = selectKey.executeQuery())
                            {
                                if (!encryptedKeyResultSet.next())
                                {
                                    throw new ImplementationError("No key found for " + rscName + "/" + vlmNr);
                                }
                                encryptedPw = encryptedKeyResultSet.getString(1);
                            }
                            updateStmt.setBytes(1, Base64.decode(encryptedPw));
                            updateStmt.setInt(2, resultSet.getInt("LAYER_RESOURCE_ID"));
                            updateStmt.setInt(3, vlmNr);

                            updateStmt.executeUpdate();
                        }
                    }
                }
            }
        }
    }

    private void update(Connection connectionRef, String sql, int newVal, int oldVal)
        throws SQLException
    {
        try (PreparedStatement stmt = connectionRef.prepareStatement(sql))
        {
            stmt.setInt(1, newVal);
            stmt.setInt(2, oldVal);
            stmt.executeUpdate();
        }
    }

    private void delete(Connection connectionRef, String sql, int duplicateIdRef)
        throws SQLException
    {
        try (PreparedStatement stmt = connectionRef.prepareStatement(sql))
        {
            stmt.setInt(1, duplicateIdRef);
            stmt.executeUpdate();
        }
    }

    private static class Triple<A, B, C>
    {
        @Nullable A objA;
        @Nullable B objB;
        @Nullable C objC;

        Triple(@Nullable A objARef, @Nullable B objBRef, @Nullable C objCRef)
        {
            objA = objARef;
            objB = objBRef;
            objC = objCRef;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((objA == null) ? 0 : objA.hashCode());
            result = prime * result + ((objB == null) ? 0 : objB.hashCode());
            result = prime * result + ((objC == null) ? 0 : objC.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean eq = obj instanceof Triple;
            if (eq)
            {
                Triple<?, ?, ?> other = (Triple<?, ?, ?>) obj;
                eq = Objects.equals(objA, other.objA) &&
                    Objects.equals(objB, other.objB) &&
                    Objects.equals(objC, other.objC);
            }
            return eq;
        }

        @Override
        public String toString()
        {
            return "Tripple [objA=" + objA + ", objB=" + objB + ", objC=" + objC + "]";
        }
    }

}
