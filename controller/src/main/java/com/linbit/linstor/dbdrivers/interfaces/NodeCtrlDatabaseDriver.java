package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

public interface NodeCtrlDatabaseDriver extends NodeDatabaseDriver,
    ControllerDatabaseDriver<Node, Node.InitMaps, Void>
{

}
