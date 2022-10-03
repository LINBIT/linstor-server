package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbcp.migration.Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps;
import com.linbit.linstor.dbcp.migration.Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps.SnapshotKey;
import com.linbit.linstor.dbcp.migration.Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps.SnapshotVolumeKey;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.transaction.EtcdTransaction;

import static com.linbit.linstor.dbcp.migration.Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps.PATH_SEPARATOR;
import static com.linbit.linstor.dbcp.migration.Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps.PATH_SNAPSHOTS;
import static com.linbit.linstor.dbcp.migration.Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps.PROPS_TBL;
import static com.linbit.linstor.dbcp.migration.Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps.SNAP_TBL;
import static com.linbit.linstor.dbcp.migration.Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps.SNAP_VLM_TBL;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@EtcdMigration(
    description = "Cleanup orphaned Snapshot and SnapshotVolume Properties",
    version = 49
)
public class Migration_22_CleanupOrphanedSnapAndSnapVlmProps extends BaseEtcdMigration
{

    @Override
    public void migrate(EtcdTransaction tx, String prefix) throws Exception
    {
        final String propsBaseKey = prefix + PROPS_TBL + PATH_SEPARATOR;
        final String snapPropsBaseKey = propsBaseKey + PATH_SNAPSHOTS;
        final String absRscBaseKey = prefix + SNAP_TBL + PATH_SEPARATOR;
        final String absVlmBaseKey = prefix + SNAP_VLM_TBL + PATH_SEPARATOR;

        Set<String> propInstancesRef = new HashSet<>();
        for (String snapPropsKey : tx.get(snapPropsBaseKey, true).keySet())
        {
            // cannot use extractPrimaryKey method here since props-EtcdKeys are special
            String instanceName = snapPropsKey;
            instanceName = instanceName.substring(propsBaseKey.length()); // cut away base prefix
            instanceName = instanceName.split(":")[0]; // ":" separates instance name from prop-key
            propInstancesRef.add(instanceName);
        }

        Set<SnapshotKey> snapKeySet = new HashSet<>();
        for (String absRscKey : tx.get(absRscBaseKey).keySet())
        {
            String[] pks = EtcdUtils.splitPks(extractPrimaryKey(absRscKey), false);
            if (pks[2] != null && !pks[2].trim().isEmpty())
            {
                // pks[2] is the snapshot name. if it is empty, we have a resource
                snapKeySet.add(new SnapshotKey(pks[0], pks[1], pks[2]));
            }
        }

        Set<SnapshotVolumeKey> snapVlmKeySet = new HashSet<>();
        for (String absVlmKey : tx.get(absVlmBaseKey).keySet())
        {
            String[] pks = EtcdUtils.splitPks(extractPrimaryKey(absVlmKey), false);
            if (pks[2] != null && !pks[2].trim().isEmpty())
            {
                // pks[2] is the snapshot name. if it is empty, we have a resource
                snapVlmKeySet.add(
                    new SnapshotVolumeKey(
                        pks[0],
                        pks[1],
                        pks[2],
                        Integer.parseInt(pks[3])
                    )
                );
            }
        }

        Collection<String> propInstancesToDeleteSet = Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps
            .getPropsInstancesToDelete(
                propInstancesRef,
                snapKeySet,
                snapVlmKeySet
                );

        for (String propInstanceToDelete : propInstancesToDeleteSet)
        {
            tx.delete(propsBaseKey + propInstanceToDelete, true);
        }
    }
}
