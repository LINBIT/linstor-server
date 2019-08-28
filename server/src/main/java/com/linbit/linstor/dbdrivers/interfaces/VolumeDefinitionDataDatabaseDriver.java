package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.VolumeDefinitionData;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link com.linbit.linstor.core.objects.VolumeDefinitionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VolumeDefinitionDataDatabaseDriver extends GenericDatabaseDriver<VolumeDefinitionData>
{
    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<VolumeDefinitionData> getStateFlagsPersistence();

    /**
     * A special sub-driver to update the persisted volumeSize.
     */
    SingleColumnDatabaseDriver<VolumeDefinitionData, Long> getVolumeSizeDriver();
}
