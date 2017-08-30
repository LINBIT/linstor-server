package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.drbdmanage.StorPoolData;
import com.linbit.drbdmanage.dbdrivers.interfaces.BaseDatabaseDriver.BasePropsDatabaseDriver;

/**
 * Database driver for {@link StorPoolData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface StorPoolDataDatabaseDriver extends BasePropsDatabaseDriver<StorPoolData>
{
    /**
     * Checks if the stored primary key already exists in the databse.
     * If it does not exist, {@link #create(Connection, StorPoolData)} is called.
     *
     * @param con
     *  The used database {@link Connection}
     * @param storPoolData
     *  The data to be stored (except the primary key)
     * @throws SQLException
     */
    public void ensureEntryExists(Connection con, StorPoolData storPoolData)
        throws SQLException;
}
