package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link SnapshotVolumeDefinition}.
 */
public interface SnapshotVolumeDefinitionDatabaseDriver extends GenericDatabaseDriver<SnapshotVolumeDefinition>
{
    /**
     * A special sub-driver to update the persisted volumeSize.
     */
    SingleColumnDatabaseDriver<SnapshotVolumeDefinition, Long> getVolumeSizeDriver();

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<SnapshotVolumeDefinition> getStateFlagsPersistence();
}
