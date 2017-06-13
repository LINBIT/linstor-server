package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.net.InetAddress;
import java.sql.Connection;

import com.linbit.MapDatabaseDriver;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.drbdmanage.NetInterface;
import com.linbit.drbdmanage.NetInterfaceName;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.StorPool;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface NodeDatabaseDriver
{
    void setConnection(Connection dbCon);

    StateFlagsPersistence getStateFlagPersistence();

    ObjectDatabaseDriver<NodeType> getNodeTypeDriver();

    ObjectDatabaseDriver<InetAddress> getNodeNetInterfaceDriver(NetInterfaceName netInterfaceName);

    MapDatabaseDriver<ResourceName, Resource> getNodeResourceMapDriver();

    MapDatabaseDriver<NetInterfaceName, NetInterface> getNodeNetInterfaceMapDriver();

    MapDatabaseDriver<StorPoolName, StorPool> getNodeStorPoolMapDriver();
}
