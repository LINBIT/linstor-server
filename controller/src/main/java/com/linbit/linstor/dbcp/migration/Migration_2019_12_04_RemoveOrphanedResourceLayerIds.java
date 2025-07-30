package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.12.04.01.01",
    description = "Delete orphaned layer resource ids"
)
public class Migration_2019_12_04_RemoveOrphanedResourceLayerIds extends LinstorMigration
{
    @Override
    public void migrate(Connection dbCon, DbProduct dbProduct) throws Exception
    {
        try
        (
            PreparedStatement select = dbCon.prepareStatement(
                " SELECT TMP.LAYER_RESOURCE_ID, TMP.NODE_NAME, TMP.RESOURCE_NAME, R.RESOURCE_FLAGS FROM ( " +
                "      SELECT ID.LAYER_RESOURCE_ID, ID.NODE_NAME, ID.RESOURCE_NAME, ID.LAYER_RESOURCE_KIND, DV.VLM_NR " +
                "      FROM LAYER_RESOURCE_IDS as ID LEFT OUTER JOIN LAYER_DRBD_VOLUMES AS DV " +
                "        ON ID.LAYER_RESOURCE_ID = DV.LAYER_RESOURCE_ID " +
                "      WHERE ID.LAYER_RESOURCE_KIND = 'DRBD')" +
                "     AS TMP" +
                "   LEFT OUTER JOIN VOLUMES V " +
                "   ON TMP.NODE_NAME = V.NODE_NAME AND TMP.RESOURCE_NAME = V.RESOURCE_NAME AND TMP.VLM_NR = V.VLM_NR " +
                "   JOIN RESOURCES R" +
                "   ON TMP.NODE_NAME = R.NODE_NAME AND TMP.RESOURCE_NAME = R.RESOURCE_NAME" +
                "   WHERE V.UUID IS NULL AND " +
                        " R.RESOURCE_FLAGS = 2" // delete
            );
            ResultSet rs = select.executeQuery();

            PreparedStatement deleteLayerRscId = dbCon.prepareStatement(
                "DELETE FROM LAYER_RESOURCE_IDS " +
                " WHERE LAYER_RESOURCE_ID = ?"
            );
            PreparedStatement deleteRsc = dbCon.prepareStatement(
                "DELETE FROM RESOURCES " +
                " WHERE NODE_NAME     = ? AND " +
                      " RESOURCE_NAME = ? "
            )
        )
        {
            while (rs.next())
            {
                deleteLayerRscId.setInt(1, rs.getInt("LAYER_RESOURCE_ID"));
                deleteLayerRscId.executeUpdate();

                deleteRsc.setString(1, rs.getString("NODE_NAME"));
                deleteRsc.setString(2, rs.getString("RESOURCE_NAME"));
                deleteRsc.executeUpdate();
            }
        }
    }
}
