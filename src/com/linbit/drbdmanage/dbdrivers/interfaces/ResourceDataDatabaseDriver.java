package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface ResourceDataDatabaseDriver
{
    public StateFlagsPersistence getStateFlagPersistence();

    public void create(Connection dbCon, ResourceData resData)
        throws SQLException;

    public ResourceData load(Connection con, Node node, SerialGenerator serialGen, TransactionMgr transMgr)
        throws SQLException;

    public void delete(Connection con, ResourceData res)
        throws SQLException;
}
