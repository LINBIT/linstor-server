package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link com.linbit.linstor.core.objects.VolumeDefinition}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VolumeDefinitionDatabaseDriver extends GenericDatabaseDriver<VolumeDefinition>
{
    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<VolumeDefinition> getStateFlagsPersistence();

    /**
     * A special sub-driver to update the persisted volumeSize.
     */
    SingleColumnDatabaseDriver<VolumeDefinition, Long> getVolumeSizeDriver();
}
