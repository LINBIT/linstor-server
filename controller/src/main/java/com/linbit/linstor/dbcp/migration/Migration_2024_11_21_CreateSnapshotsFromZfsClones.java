package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

@SuppressWarnings({"checkstyle:typename", "checkstyle:magicnumber"})
@Migration(
    version = "2024.11.21.10.00",
    description = "Add property for satellites to mark CF_ snapshots for deletion"
)
public class Migration_2024_11_21_CreateSnapshotsFromZfsClones extends LinstorMigration
{
    public static final String TBL_PROPS_CON = "PROPS_CONTAINERS";
    public static final String CLM_PROPS_INSTANCE = "PROPS_INSTANCE";
    public static final String CLM_PROP_KEY = "PROP_KEY";
    public static final String CLM_PROP_VALUE = "PROP_VALUE";

    public static final String TBL_NODE_STOR_POOL = "NODE_STOR_POOL";
    public static final String CLM_NODES_NAME = "NODE_NAME";

    public static final String PROP_KEY_STLT_MIGRATION =
        "Satellite/Migrations/2024_11_21_MarkZfsCF_SnapshotsForDeletion/NeedsAction";
    public static final String PROP_VALUE_STLT_MIGRATION = "True";

    private static final String SQL_INSERT = "INSERT INTO " + TBL_PROPS_CON + " (" +
        CLM_PROPS_INSTANCE + ", " + CLM_PROP_KEY + ", " + CLM_PROP_VALUE +
        ") VALUES (?, ?, ?)";

    private static final String SQL_SELECT_NODES = "SELECT DISTINCT " + CLM_NODES_NAME + " FROM " + TBL_NODE_STOR_POOL +
        " WHERE DRIVER_NAME IN ('" + DeviceProviderKind.ZFS_THIN.name() + "', '" +
                                     DeviceProviderKind.ZFS.name() + "')";

    @Override
    public void migrate(Connection conRef, DbProduct dbProduct) throws Exception
    {
        try (
            PreparedStatement selectStmt = conRef.prepareStatement(SQL_SELECT_NODES);
            ResultSet rs = selectStmt.executeQuery();
            PreparedStatement insertStmt = conRef.prepareStatement(SQL_INSERT);
        )
        {
            insertStmt.setString(2, PROP_KEY_STLT_MIGRATION);
            insertStmt.setString(3, PROP_VALUE_STLT_MIGRATION);
            while (rs.next())
            {
                insertStmt.setString(1, "/NODES/" + rs.getString(CLM_NODES_NAME));
                insertStmt.executeUpdate();
            }
        }
    }
}
