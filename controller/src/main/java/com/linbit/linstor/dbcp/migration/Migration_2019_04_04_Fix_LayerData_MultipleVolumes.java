package com.linbit.linstor.dbcp.migration;

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

    @Override
    protected void migrate(Connection connectionRef) throws Exception
    {
        Map<Tripple<String, String, String>, Integer> layerIds = new HashMap<>();

        try (PreparedStatement stmt = connectionRef.prepareStatement(SELECT_ALL_LAYER_RESOURCE_IDS))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    Tripple<String, String, String> key = new Tripple<>(
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
                        update(connectionRef, UPDATE_DUP_LUKS_VOLUME, origId, duplicateId);
                        update(connectionRef, UPDATE_DUP_STORAGE_VOLUME, origId, duplicateId);
                        delete(connectionRef, DELETE_DUP_DRBD_RESOURCE, duplicateId);
                        delete(connectionRef, DELETE_DUP_LAYER_RESOURCE_ID, duplicateId);
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

    private static class Tripple<A, B, C>
    {
        A objA;
        B objB;
        C objC;

        Tripple(A objARef, B objBRef, C objCRef)
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
            boolean eq = obj != null && obj instanceof Tripple;
            if (eq)
            {
                Tripple<?, ?, ?> other = (Tripple<?, ?, ?>) obj;
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
