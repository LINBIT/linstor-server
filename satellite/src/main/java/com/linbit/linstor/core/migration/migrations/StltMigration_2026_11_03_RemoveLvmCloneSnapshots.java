package com.linbit.linstor.core.migration.migrations;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.migration.BaseStltMigration;
import com.linbit.linstor.core.migration.SatelliteMigrations;
import com.linbit.linstor.core.migration.StltMigration;
import com.linbit.linstor.core.migration.StltMigrationHandler.StltMigrationResult;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.layer.DeviceLayerUtils;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LvmVolumeType;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.StringUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Removes all LVs tagged "linstor_clone_snapshot". Clones used to take these snapshots of their source themselves and
 * delete them when the clone was done. Clones now use a regular LINSTOR snapshot, so a remaining one is left over from
 * a clone that was interrupted (e.g. by the upgrade) or failed to clean up. A thick one also slows down every write to
 * its origin.
 * <p>
 * The tag is what identifies them: a LINSTOR snapshot "CF_..." of a clone has the same LV name pattern. They were
 * created in the volume group of the storage pool of the clone source, so only the volume groups of the LVM storage
 * pools of this node are searched, as usual with the LVM filter of LINSTOR (see {@link LvmUtils#execWithRetry}).
 * <p>
 * Failures are only reported: nothing needs these snapshots, so they must not keep the satellite from going online.
 */
@StltMigration(migration = SatelliteMigrations.MIG_REMOVE_LVM_CLONE_SNAPS)
@Singleton
@SuppressWarnings("checkstyle:typename")
public class StltMigration_2026_11_03_RemoveLvmCloneSnapshots extends BaseStltMigration
{
    static final String LVM_TAG_CLONE_SNAPSHOT = "linstor_clone_snapshot";

    private final ErrorReporter errorReporter;

    @Inject
    public StltMigration_2026_11_03_RemoveLvmCloneSnapshots(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
    }

    @Override
    public StltMigrationResult migrate(ExtCmdFactory extCmdFactoryRef, Node localNodeRef)
    {
        for (String volumeGroup : getLvmVolumeGroups(localNodeRef))
        {
            try
            {
                for (String lvName : listCloneSnapshots(extCmdFactoryRef, volumeGroup))
                {
                    removeCloneSnapshot(extCmdFactoryRef, volumeGroup, lvName);
                }
            }
            catch (StorageException exc)
            {
                errorReporter.reportError(exc);
            }
        }
        LvmUtils.recacheNextLvs();
        return createDefaultSuccessResult();
    }

    private static Set<String> getLvmVolumeGroups(Node localNode)
    {
        Set<String> ret = new TreeSet<>();
        localNode.streamStorPools()
            .filter(
                storPool -> storPool.getDeviceProviderKind() == DeviceProviderKind.LVM ||
                    storPool.getDeviceProviderKind() == DeviceProviderKind.LVM_THIN
            )
            .forEach(storPool ->
            {
                @Nullable String volumeGroup = getVolumeGroup(storPool);
                if (volumeGroup != null)
                {
                    ret.add(volumeGroup);
                }
            });
        return ret;
    }

    /** The volume group of an LVM storage pool, or of an LVM_THIN one (named {@code <vg>/<thin pool>}) */
    private static @Nullable String getVolumeGroup(StorPool storPool)
    {
        @Nullable String volumeGroup;
        try
        {
            @Nullable String storPoolName = DeviceLayerUtils.getNamespaceStorDriver(storPool.getReadOnlyProps())
                .getProp(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY);
            volumeGroup = storPoolName == null ? null : StringUtils.split(storPoolName, "/")[0];
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return volumeGroup;
    }

    private static List<String> listCloneSnapshots(ExtCmdFactory extCmdFactoryRef, String volumeGroup)
        throws StorageException
    {
        OutputData lvs = LvmUtils.execWithRetry(
            extCmdFactoryRef,
            Collections.singleton(volumeGroup),
            config -> LvmCommands.lvsWithTag(extCmdFactoryRef.create(), volumeGroup, LVM_TAG_CLONE_SNAPSHOT, config)
        );
        List<String> ret = new ArrayList<>();
        for (String line : StringUtils.split(new String(lvs.stdoutData, StandardCharsets.UTF_8), "\n"))
        {
            String lvName = line.trim();
            if (!lvName.isEmpty())
            {
                ret.add(lvName);
            }
        }
        return ret;
    }

    private void removeCloneSnapshot(ExtCmdFactory extCmdFactoryRef, String volumeGroup, String lvName)
    {
        errorReporter.logInfo("Removing leftover clone snapshot %s/%s", volumeGroup, lvName);
        try
        {
            LvmUtils.execWithRetry(
                extCmdFactoryRef,
                Collections.singleton(volumeGroup),
                config -> LvmCommands.delete(
                    extCmdFactoryRef.create(),
                    volumeGroup,
                    lvName,
                    config,
                    LvmVolumeType.SNAPSHOT
                )
            );
        }
        catch (StorageException exc)
        {
            errorReporter.logWarning(
                "Failed to remove leftover clone snapshot %s/%s, it has to be removed manually: %s",
                volumeGroup,
                lvName,
                exc.getMessage()
            );
        }
    }
}
