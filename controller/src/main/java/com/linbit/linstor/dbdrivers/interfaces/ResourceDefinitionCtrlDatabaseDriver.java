package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

import java.util.Map;

public interface ResourceDefinitionCtrlDatabaseDriver extends ResourceDefinitionDatabaseDriver,
    ControllerDatabaseDriver<ResourceDefinition, ResourceDefinition.InitMaps, Map<ResourceGroupName, ResourceGroup>>
{

}
