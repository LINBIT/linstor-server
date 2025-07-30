package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.11.12.01.01",
    description = "Split Resource.Flags.DISKLESS into DRBD_DISKLESS and NVME_INITIATOR"
)
public class Migration_2019_11_12_DisklessFlagSplit extends LinstorMigration
{
    private static final long FLAG_DISKLESS = 1L << 2;
    private static final long FLAG_DRBD_DISKLESS = FLAG_DISKLESS | 1L << 8;
    private static final long FLAG_NVME_INITIATOR = FLAG_DISKLESS | 1L << 9;

    private static final String TBL_LAYER_RESOURCE_IDS = "LAYER_RESOURCE_IDS";
    private static final String TBL_RESOURCES = "RESOURCES";

    private static final String NODE_NAME = "NODE_NAME";
    private static final String RSC_NAME = "RESOURCE_NAME";
    private static final String RSC_FLAGS = "RESOURCE_FLAGS";
    private static final String LAYER_RESOURCE_KIND = "LAYER_RESOURCE_KIND";

    private static final String KIND_DRBD = "DRBD";
    private static final String KIND_NVME = "NVME";

    @Override
    public void migrate(Connection dbCon, DbProduct dbProduct) throws Exception
    {
        String selectRscs =
            "SELECT " +
                "R." + NODE_NAME +
                ", R." + RSC_NAME +
                ", R." + RSC_FLAGS +
                ", L." + LAYER_RESOURCE_KIND +
            " FROM " + TBL_RESOURCES + " AS R, " + TBL_LAYER_RESOURCE_IDS + " AS L " +
            " WHERE R." + NODE_NAME + " = L." + NODE_NAME + " AND " +
                   "R." + RSC_NAME +  " = L." + RSC_NAME;

        String updateRsc =
            "UPDATE " + TBL_RESOURCES +
            " SET " + RSC_FLAGS + " = ? " +
            " WHERE " + NODE_NAME + " = ? AND " +
                        RSC_NAME +  " = ?";

        try (
            PreparedStatement selectStmt = dbCon.prepareStatement(selectRscs);
            PreparedStatement updateStmt = dbCon.prepareStatement(updateRsc);
            ResultSet resultSet = selectStmt.executeQuery();
        )
        {
            while (resultSet.next())
            {
                long flags = resultSet.getLong(RSC_FLAGS);
                if ((flags & FLAG_DISKLESS) == FLAG_DISKLESS)
                {
                    boolean needsUpdate = true;
                    switch (resultSet.getString(LAYER_RESOURCE_KIND))
                    {
                        case KIND_DRBD:
                            flags |= FLAG_DRBD_DISKLESS;
                            break;
                        case KIND_NVME:
                            flags |= FLAG_NVME_INITIATOR;
                            break;
                        default:
                            needsUpdate = false;
                            // ignore
                            break;
                    }
                    if (needsUpdate)
                    {
                        updateStmt.setLong(1, flags);
                        updateStmt.setString(2, resultSet.getString(NODE_NAME));
                        updateStmt.setString(3, resultSet.getString(RSC_NAME));

                        updateStmt.executeUpdate();
                    }
                }
            }
        }
    }
}
