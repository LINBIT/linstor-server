package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

import java.util.Map;

public interface NetInterfaceCtrlDatabaseDriver extends NetInterfaceDatabaseDriver,
    ControllerDatabaseDriver<NetInterface, Void, Map<NodeName, ? extends Node>>
{

}
