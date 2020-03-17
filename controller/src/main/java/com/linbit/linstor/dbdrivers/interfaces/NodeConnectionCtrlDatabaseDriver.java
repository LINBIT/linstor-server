package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

import java.util.Map;

public interface NodeConnectionCtrlDatabaseDriver extends NodeConnectionDatabaseDriver,
    ControllerDatabaseDriver<NodeConnection, Void, Map<NodeName, ? extends Node>>
{

}
