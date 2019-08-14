package com.linbit.linstor.dbcp.migration;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2018.12.13.14.32",
    description = "Add NODE_ID column to SNAPSHOTS"
)
public class Migration_2018_12_13_SnapshotNodeId extends LinstorMigration
{
    private static final String TBL_SNAPSHOT = "SNAPSHOTS";

    private static final String S_NODE_NAME = "NODE_NAME";
    private static final String S_RES_NAME = "RESOURCE_NAME";
    private static final String S_NAME = "SNAPSHOT_NAME";
    private static final String S_NODE_ID = "NODE_ID";

    private static final String S_SELECT_ALL =
        " SELECT " + S_NODE_NAME + ", " + S_RES_NAME + ", " + S_NAME + ", " + S_NODE_ID +
            " FROM " + TBL_SNAPSHOT;

    private static final String TBL_RES = "RESOURCES";

    private static final String RES_NODE_NAME = "NODE_NAME";
    private static final String RES_NAME = "RESOURCE_NAME";
    private static final String RES_NODE_ID = "NODE_ID";

    private static final String RES_SELECT_ALL =
        " SELECT " + RES_NODE_NAME + ", " + RES_NAME + ", " + RES_NODE_ID + " FROM " + TBL_RES;

    private static final String TBL_SNAPSHOT_DFN = "SNAPSHOT_DEFINITIONS";

    private static final String SD_RES_NAME = "RESOURCE_NAME";
    private static final String SD_NAME = "SNAPSHOT_NAME";
    private static final String SD_FLAGS = "SNAPSHOT_FLAGS";

    private static final String SD_SELECT_ALL =
        " SELECT " + SD_RES_NAME + ", " + SD_NAME + ", " + SD_FLAGS +
            " FROM " + TBL_SNAPSHOT_DFN;

    private static final long SD_FLAG_SUCCESSFUL = 1L;
    private static final long SD_FLAG_FAILED_DEPLOYMENT = 2L;

    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct)
        throws Exception
    {
        if (!MigrationUtils.columnExists(connection, TBL_SNAPSHOT, S_NODE_ID))
        {
            SQLUtils.runSql(connection,
                "ALTER TABLE " + TBL_SNAPSHOT  + " ADD COLUMN " + S_NODE_ID + " INTEGER NOT NULL DEFAULT 0;");

            setNodeIds(connection);
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void setNodeIds(Connection connection)
        throws Exception
    {
        // loop through SNAPSHOTS
        // try to find origin resource
        //   if exists, replace node ID
        //   if not, mark  snapshot definition as failed

        Map<Tuple2<String, String>, Integer> resourceNodeIds = resourceNodeIds(connection);
        Set<Tuple2<String, String>> failedSnapshotDefinitions = new HashSet<>();

        ResultSet resultSet = connection.createStatement(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE).executeQuery(S_SELECT_ALL);
        while (resultSet.next())
        {
            String nodeName = resultSet.getString(S_NODE_NAME);
            String rscName = resultSet.getString(S_RES_NAME);
            String snapshotName = resultSet.getString(S_NAME);
            Integer nodeId = resourceNodeIds.get(Tuples.of(nodeName, rscName));

            if (nodeId == null)
            {
                // We have no idea what the node ID should be, so mark the snapshot as failed so that it cannot be
                // restored.
                failedSnapshotDefinitions.add(Tuples.of(rscName, snapshotName));
            }
            else
            {
                // Assume that the current resource is the resource from which the snapshot was taken.
                // It is possible that this might be incorrect if the resource has been deleted and re-created
                // since the snapshot was taken.
                resultSet.updateInt(S_NODE_ID, nodeId);
                resultSet.updateRow();
            }
        }
        resultSet.close();

        markFailed(connection, failedSnapshotDefinitions);
    }

    private Map<Tuple2<String, String>, Integer> resourceNodeIds(Connection connection)
        throws Exception
    {
        Map<Tuple2<String, String>, Integer> resourceNodeIds = new HashMap<>();
        ResultSet resultSet = connection.createStatement().executeQuery(RES_SELECT_ALL);
        while (resultSet.next())
        {
            String nodeName = resultSet.getString(RES_NODE_NAME);
            String rscName = resultSet.getString(RES_NAME);
            int nodeId = resultSet.getInt(RES_NODE_ID);
            resourceNodeIds.put(Tuples.of(nodeName, rscName), nodeId);
        }
        resultSet.close();
        return resourceNodeIds;
    }

    private void markFailed(Connection connection, Collection<Tuple2<String, String>> failedSnapshotDefinitions)
        throws Exception
    {
        ResultSet resultSet = connection.createStatement(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE).executeQuery(SD_SELECT_ALL);
        while (resultSet.next())
        {
            String rscName = resultSet.getString(SD_RES_NAME);
            String snapshotName = resultSet.getString(SD_NAME);
            if (failedSnapshotDefinitions.contains(Tuples.of(rscName, snapshotName)))
            {
                resultSet.updateLong(SD_FLAGS,
                    (resultSet.getLong(SD_FLAGS) & ~SD_FLAG_SUCCESSFUL) | SD_FLAG_FAILED_DEPLOYMENT
                );
                resultSet.updateRow();
            }
        }
        resultSet.close();
    }
}
