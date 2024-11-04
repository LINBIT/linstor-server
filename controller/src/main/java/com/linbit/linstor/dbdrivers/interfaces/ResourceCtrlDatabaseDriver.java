package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.utils.PairNonNull;

import java.util.Map;

public interface ResourceCtrlDatabaseDriver extends ResourceDatabaseDriver,
    ControllerDatabaseDriver<AbsResource<Resource>,
        Resource.InitMaps,
        PairNonNull<Map<NodeName, Node>, Map<ResourceName, ResourceDefinition>>>
{

}
