package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.utils.Pair;

import java.util.Map;

public interface ResourceConnectionCtrlDatabaseDriver extends ResourceConnectionDatabaseDriver,
    ControllerDatabaseDriver<ResourceConnection, Void, Map<Pair<NodeName, ResourceName>, ? extends Resource>>
{

}
