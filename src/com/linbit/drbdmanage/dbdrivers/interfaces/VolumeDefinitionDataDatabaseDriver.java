package com.linbit.drbdmanage.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.VolumeDefinition.VlmDfnFlags;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.dbdrivers.interfaces.BaseDatabaseDriver.BasePropsDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link VolumeDefinitionData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface VolumeDefinitionDataDatabaseDriver extends BasePropsDatabaseDriver<VolumeDefinitionData>
{
    /**
     * A special sub-driver to update the persisted {@link VlmDfnFlags}s. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public StateFlagsPersistence getStateFlagsPersistence();

    /**
     * A special sub-driver to update the persisted minorNumber. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public SingleColumnDatabaseDriver<MinorNumber> getMinorNumberDriver();

    /**
     * A special sub-driver to update the persisted volumeSize. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public SingleColumnDatabaseDriver<Long> getVolumeSizeDriver();

    /**
     * A special sub-driver to update the instance specific {@link Props}. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public PropsConDatabaseDriver getPropsDriver();
}
