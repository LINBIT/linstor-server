package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface NodeDataDatabaseDriver
{
    public StateFlagsPersistence getStateFlagPersistence();

    public StateFlagsPersistence getNodeTypeStateFlagPersistence();

    public PropsConDatabaseDriver getPropsConDriver();

    public void create(Connection con, NodeData nodeData)
        throws SQLException;

    public NodeData load(Connection con, SerialGenerator serialGen, TransactionMgr transMgr)
        throws SQLException;

    void delete(Connection con, NodeData node)
        throws SQLException;
}
