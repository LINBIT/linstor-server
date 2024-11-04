package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.utils.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Migration(
    version = "2022.10.03.09.00",
    description = "Cleanup orphaned Snapshot and SnapshotVolume Properties"
)
public class Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps extends LinstorMigration
{
    public static final String PATH_SEPARATOR = "/";

    public static final String PROPS_TBL = "PROPS_CONTAINERS";
    public static final String PROPS_INST = "PROPS_INSTANCE";

    public static final String PATH_SNAPSHOTS = "/snapshots/";

    public static final String SNAP_TBL = "RESOURCES";
    public static final String SNAP_VLM_TBL = "VOLUMES";
    private static final String SNAP_NODE_NAME = "NODE_NAME";
    private static final String SNAP_RSC_NAME = "RESOURCE_NAME";
    private static final String SNAP_SNAP_NAME = "SNAPSHOT_NAME";
    private static final String SNAP_VLM_NR = "VLM_NR";

    @Override
    protected void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        try (
            PreparedStatement propsStmt = connection.prepareStatement(
                "SELECT " + StringUtils.join(", ", PROPS_INST) +
                " FROM " + PROPS_TBL +
                    " WHERE " + PROPS_INST + " like '" + PATH_SNAPSHOTS.toUpperCase() + "%'"
            );

            PreparedStatement absRscStmt = connection.prepareStatement(
                "SELECT " + StringUtils.join(", ", SNAP_NODE_NAME, SNAP_RSC_NAME, SNAP_SNAP_NAME) +
                " FROM " + SNAP_TBL
            );

            PreparedStatement absVlmStmt = connection.prepareStatement(
                "SELECT " + StringUtils.join(", ", SNAP_NODE_NAME, SNAP_RSC_NAME, SNAP_SNAP_NAME, SNAP_VLM_NR) +
                " FROM " + SNAP_VLM_TBL
            );

            ResultSet propsResult = propsStmt.executeQuery();
            ResultSet absRscResult = absRscStmt.executeQuery();
            ResultSet absVlmResult = absVlmStmt.executeQuery();

            PreparedStatement deletePropStmt = connection.prepareStatement(
                "DELETE FROM " + PROPS_TBL + " WHERE " + PROPS_INST + " = ?"
            );
        )
        {
            Set<SnapshotKey> snapKeySet = new HashSet<>();
            Set<SnapshotVolumeKey> snapVlmKeyset = new HashSet<>();
            Set<String> propInstances = new HashSet<>();

            while (propsResult.next())
            {
                propInstances.add(propsResult.getString(PROPS_INST));
            }
            while (absRscResult.next())
            {
                snapKeySet.add(
                    new SnapshotKey(
                        absRscResult.getString(SNAP_NODE_NAME),
                        absRscResult.getString(SNAP_RSC_NAME),
                        absRscResult.getString(SNAP_SNAP_NAME)
                    )
                );
            }
            while (absVlmResult.next())
            {
                snapVlmKeyset.add(
                    new SnapshotVolumeKey(
                        absVlmResult.getString(SNAP_NODE_NAME),
                        absVlmResult.getString(SNAP_RSC_NAME),
                        absVlmResult.getString(SNAP_SNAP_NAME),
                        absVlmResult.getInt(SNAP_VLM_NR)
                    )
                );
            }

            Collection<String> propsInstancesToDelete = getPropsInstancesToDelete(
                propInstances,
                snapKeySet,
                snapVlmKeyset
            );

            for (String propsInstanceToDelete : propsInstancesToDelete)
            {
                deletePropStmt.setString(1, propsInstanceToDelete);
                deletePropStmt.execute();
            }
        }
    }

    public static Collection<String> getPropsInstancesToDelete(
        Set<String> propInstancesRef,
        Set<SnapshotKey> snapKeySetRef,
        Set<SnapshotVolumeKey> snapVlmKeysetRef
    )
    {
        HashMap<String, String> propsInstancesToDelete = new HashMap<>();
        for (String propInstance : propInstancesRef)
        {
            propsInstancesToDelete.put(propInstance.toLowerCase(), propInstance);
        }

        for (SnapshotKey snapKey : snapKeySetRef)
        {
            propsInstancesToDelete.remove(snapKey.buildPath().toLowerCase());
        }
        for (SnapshotVolumeKey snapVlmKey : snapVlmKeysetRef)
        {
            propsInstancesToDelete.remove(snapVlmKey.buildPath().toLowerCase());
        }

        return propsInstancesToDelete.values();
    }

    public static class SnapshotKey
    {
        public final String nodeName;
        public final String rscName;
        public final String snapName;

        public SnapshotKey(String nodeNameRef, String rscNameRef, String snapNameRef)
        {
            nodeName = nodeNameRef;
            rscName = rscNameRef;
            snapName = snapNameRef;
        }

        public String buildPath()
        {
            return PATH_SNAPSHOTS + nodeName +
                PATH_SEPARATOR + rscName +
                PATH_SEPARATOR + snapName;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + nodeName.hashCode();
            result = prime * result + rscName.hashCode();
            result = prime * result + snapName.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean ret = false;
            if (obj instanceof SnapshotKey)
            {
                SnapshotKey other = (SnapshotKey) obj;
                ret = Objects.equals(nodeName, other.nodeName) &&
                    Objects.equals(rscName, other.rscName) &&
                    Objects.equals(snapName, other.snapName);
            }
            return ret;
        }

    }

    public static class SnapshotVolumeKey
    {
        public final String nodeName;
        public final String rscName;
        public final int vlmNr;
        public final String snapName;

        public SnapshotVolumeKey(String nodeNameRef, String rscNameRef, String snapNameRef, int vlmNrRef)
        {
            nodeName = nodeNameRef;
            rscName = rscNameRef;
            vlmNr = vlmNrRef;
            snapName = snapNameRef;
        }

        public String buildPath()
        {
            return PATH_SNAPSHOTS + nodeName +
                PATH_SEPARATOR + rscName +
                PATH_SEPARATOR + snapName +
                PATH_SEPARATOR + vlmNr;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + nodeName.hashCode();
            result = prime * result + rscName.hashCode();
            result = prime * result + snapName.hashCode();
            result = prime * result + vlmNr;
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean ret = false;
            if (obj instanceof SnapshotVolumeKey)
            {
                SnapshotVolumeKey other = (SnapshotVolumeKey) obj;
                ret = Objects.equals(nodeName, other.nodeName) &&
                    Objects.equals(rscName, other.rscName) &&
                    Objects.equals(vlmNr, other.vlmNr) &&
                    Objects.equals(snapName, other.snapName);
            }
            return ret;
        }
    }
}
