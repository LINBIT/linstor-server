package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.drbdmanage.ResourceDefinition.RscDfnFlags;
import com.linbit.drbdmanage.ResourceDefinitionData;
import com.linbit.drbdmanage.dbdrivers.interfaces.BaseDatabaseDriver.BasePropsDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link ResourceDefinitionData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface ResourceDefinitionDataDatabaseDriver extends BasePropsDatabaseDriver<ResourceDefinitionData>
{
    /**
     * A special sub-driver to update the persisted {@link RscDfnFlags}. The data record
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

    /**
     * Checks if the stored primary key already exists in the database.
     *
     * @param dbCon
     *  The used database {@link Connection}
     * @return
     *  True if the data record exists. False otherwise.
     * @throws SQLException
     */
    public boolean exists(Connection dbCon)
        throws SQLException;
}
