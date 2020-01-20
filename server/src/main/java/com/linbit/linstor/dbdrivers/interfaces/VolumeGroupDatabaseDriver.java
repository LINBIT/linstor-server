package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

public interface VolumeGroupDatabaseDriver extends GenericDatabaseDriver<VolumeGroup>
{
    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<VolumeGroup> getStateFlagsPersistence();
}
