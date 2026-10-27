package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Clones used to take their own storage snapshots of the clone source, now they use a regular LINSTOR snapshot. This
 * removes what clones that were interrupted (e.g. by the upgrade) or failed to clean up left behind:
 * <ul>
 *  <li>the LVM snapshots, removed by a satellite migration on every node with an LVM storage pool (ZFS ones are
 *      already marked for deletion)</li>
 *  <li>the "Clone/CF_&lt;clone target&gt;" properties on the resources of the clone source</li>
 * </ul>
 */
@SuppressWarnings({"checkstyle:typename", "checkstyle:magicnumber"})
@Migration(
    version = "2026.11.03.10.00",
    description = "Clean up the leftovers of clones that took their own source snapshots"
)
public class Migration_2026_11_03_CleanupOldCloneSnapshots extends LinstorMigration
{
    public static final String PROP_KEY_STLT_MIGRATION =
        "Satellite/Migrations/2026_11_03_RemoveLvmCloneSnapshots/NeedsAction";
    public static final String PROP_VALUE_STLT_MIGRATION = "True";

    public static final String PROPS_INSTANCE_PREFIX_RSC = "/RSCS/";
    public static final String PROP_KEY_PREFIX_OLD_CLONE = "Clone/CF_";

    private static final String SQL_SELECT_LVM_NODES = "SELECT DISTINCT NODE_NAME FROM NODE_STOR_POOL " +
        "WHERE DRIVER_NAME IN ('" + DeviceProviderKind.LVM.name() + "', '" + DeviceProviderKind.LVM_THIN.name() + "')";
    private static final String SQL_INSERT_PROP = "INSERT INTO PROPS_CONTAINERS " +
        "(PROPS_INSTANCE, PROP_KEY, PROP_VALUE) VALUES (?, ?, ?)";
    // filtered exactly in Java: "_" is a wildcard of LIKE
    private static final String SQL_SELECT_CLONE_PROPS = "SELECT PROPS_INSTANCE, PROP_KEY FROM PROPS_CONTAINERS " +
        "WHERE PROPS_INSTANCE LIKE '" + PROPS_INSTANCE_PREFIX_RSC + "%' AND PROP_KEY LIKE 'Clone/%'";
    private static final String SQL_DELETE_PROP = "DELETE FROM PROPS_CONTAINERS " +
        "WHERE PROPS_INSTANCE = ? AND PROP_KEY = ?";

    @Override
    public void migrate(Connection conRef, DbProduct dbProduct) throws Exception
    {
        try (
            PreparedStatement selectNodesStmt = conRef.prepareStatement(SQL_SELECT_LVM_NODES);
            ResultSet nodes = selectNodesStmt.executeQuery();
            PreparedStatement insertStmt = conRef.prepareStatement(SQL_INSERT_PROP);
        )
        {
            insertStmt.setString(2, PROP_KEY_STLT_MIGRATION);
            insertStmt.setString(3, PROP_VALUE_STLT_MIGRATION);
            while (nodes.next())
            {
                insertStmt.setString(1, "/NODES/" + nodes.getString("NODE_NAME"));
                insertStmt.executeUpdate();
            }
        }

        // props instance, prop key
        List<String[]> clonePropsToDelete = new ArrayList<>();
        try (
            PreparedStatement selectPropsStmt = conRef.prepareStatement(SQL_SELECT_CLONE_PROPS);
            ResultSet props = selectPropsStmt.executeQuery();
        )
        {
            while (props.next())
            {
                String propKey = props.getString("PROP_KEY");
                if (propKey.startsWith(PROP_KEY_PREFIX_OLD_CLONE))
                {
                    clonePropsToDelete.add(new String[] {props.getString("PROPS_INSTANCE"), propKey});
                }
            }
        }
        try (PreparedStatement deleteStmt = conRef.prepareStatement(SQL_DELETE_PROP))
        {
            for (String[] prop : clonePropsToDelete)
            {
                deleteStmt.setString(1, prop[0]);
                deleteStmt.setString(2, prop[1]);
                deleteStmt.executeUpdate();
            }
        }
    }
}
