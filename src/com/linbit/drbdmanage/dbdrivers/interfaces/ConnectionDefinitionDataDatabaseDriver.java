package com.linbit.drbdmanage.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.drbdmanage.ConnectionDefinitionData;
import com.linbit.drbdmanage.dbdrivers.interfaces.BaseDatabaseDriver.BasePropsDatabaseDriver;

/**
 * Database driver for {@link ConnectionDefinitionData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface ConnectionDefinitionDataDatabaseDriver extends BasePropsDatabaseDriver<ConnectionDefinitionData>
{
    /**
     * A special sub-driver to update the persisted connectionNumber. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public SingleColumnDatabaseDriver<Integer> getConnectionNumberDriver();

}
