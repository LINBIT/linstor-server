package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.MapDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.NetInterface;
import com.linbit.drbdmanage.NetInterfaceName;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.StorPool;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface NodeDataDatabaseDriver
{
    public StateFlagsPersistence getStateFlagPersistence(NodeName nodeName);

    public StateFlagsPersistence getNodeTypeStateFlagPersistence(NodeName nodeName);

    public MapDatabaseDriver<ResourceName, Resource> getNodeResourceMapDriver(NodeName nodeName);

    public MapDatabaseDriver<NetInterfaceName, NetInterface> getNodeNetInterfaceMapDriver(Node node);

    public MapDatabaseDriver<StorPoolName, StorPool> getNodeStorPoolMapDriver(Node node);

    public PropsConDatabaseDriver getPropsConDriver(NodeName nodeName);

    public void create(Connection con, NodeData nodeData)
        throws SQLException;

    public NodeData load(Connection con, NodeName nodeName, AccessContext accCtx, SerialGenerator serialGen, TransactionMgr transMgr)
        throws SQLException, AccessDeniedException;
}
