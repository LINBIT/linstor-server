package com.linbit.drbdmanage.stateflags;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Updates the state flags of a drbdmanage core object in the database
 * whenever the flags are changed
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StateFlagsPersistence
{
    void persist(Connection dbConn) throws SQLException;
}
