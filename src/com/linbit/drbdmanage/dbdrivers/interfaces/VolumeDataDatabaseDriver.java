package com.linbit.drbdmanage.dbdrivers.interfaces;

import com.linbit.drbdmanage.Volume.VlmFlags;
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.dbdrivers.interfaces.BaseDatabaseDriver.BasePropsDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link VolumeData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface VolumeDataDatabaseDriver extends BasePropsDatabaseDriver<VolumeData>
{
    /**
     * A special sub-driver to update the persisted {@link VlmFlags}. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public StateFlagsPersistence getStateFlagsPersistence();

    /**
     * A special sub-driver to update the instance specific {@link Props}. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public PropsConDatabaseDriver getPropsConDriver();
}
