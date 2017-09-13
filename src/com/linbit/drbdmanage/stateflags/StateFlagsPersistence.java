package com.linbit.drbdmanage.stateflags;

import java.sql.SQLException;

import com.linbit.TransactionMgr;

/**
 * Updates the state flags of a drbdmanage core object in the database
 * whenever the flags are changed
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StateFlagsPersistence<PK>
{
    void persist(PK primaryKey, long flags, TransactionMgr transMgr) throws SQLException;
}
