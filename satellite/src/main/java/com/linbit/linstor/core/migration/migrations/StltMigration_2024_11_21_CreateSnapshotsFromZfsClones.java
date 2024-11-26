package com.linbit.linstor.core.migration.migrations;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.core.migration.BaseStltMigration;
import com.linbit.linstor.core.migration.SatelliteMigrations;
import com.linbit.linstor.core.migration.StltMigration;
import com.linbit.linstor.core.migration.StltMigrationHandler.StltMigrationResult;
import com.linbit.linstor.core.objects.Node;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * For all ZFS snapshots that start with "cf_" (case-insensitive) the ZFS user property
 * "linstor:mark_for_deletion=true" will be set
 */
@StltMigration(migration = SatelliteMigrations.MIG_MARK_ZFS_CF_SNAPS_FOR_DELETION)
@Singleton
@SuppressWarnings("checkstyle:typename")
public class StltMigration_2024_11_21_CreateSnapshotsFromZfsClones extends BaseStltMigration
{
    private static final String CLONE_FROM_PREFIX = "cf_"; // will be treated case-insensitive

    @Inject
    public StltMigration_2024_11_21_CreateSnapshotsFromZfsClones()
    {
    }

    @Override
    public StltMigrationResult migrate(ExtCmdFactory extCmdFactoryRef, Node localNodeRef)
        throws ChildProcessTimeoutException, IOException
    {
        String stdOut = BaseStltMigration.extCmd(
            extCmdFactoryRef,
            "zfs",
            "list",
            "-t", "snapshot",
            "-o", "name,clones",
            "-H" // no heading
        );
        Map<String, Set<String>> data = split(stdOut);

        for (Map.Entry<String, Set<String>> entry : data.entrySet())
        {
            if (!entry.getValue().isEmpty())
            {
                String zfsId = entry.getKey();
                String[] zfsIdParts = zfsId.split("@");
                // entry must be a snapshot since we filtered for "-t snapshot"
                String snapName = zfsIdParts[1];
                if (snapName.toLowerCase().startsWith(CLONE_FROM_PREFIX))
                {
                    BaseStltMigration.extCmd(extCmdFactoryRef, "zfs", "set", "linstor:marked_for_deletion=true", zfsId);
                }
            }
        }
        return createDefaultSuccessResult();
    }

    private Map<String, Set<String>> split(String stdOutRef)
    {
        Map<String, Set<String>> ret = new HashMap<>();
        String[] lines = stdOutRef.split("\n");
        for (String line : lines)
        {
            String[] columns = line.split("\\s+");
            String snapId = columns[0];
            Set<String> clones = new HashSet<>();
            if (columns.length == 2)
            {
                String clonesColumn = columns[1];
                clones.addAll(Arrays.asList(clonesColumn.split(",")));
            }

            ret.put(snapId, clones);
        }
        return ret;
    }
}
