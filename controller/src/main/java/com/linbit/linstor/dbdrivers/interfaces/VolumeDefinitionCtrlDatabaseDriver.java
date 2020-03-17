package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

import java.util.Map;

public interface VolumeDefinitionCtrlDatabaseDriver extends VolumeDefinitionDatabaseDriver,
    ControllerDatabaseDriver<VolumeDefinition, VolumeDefinition.InitMaps, Map<ResourceName, ResourceDefinition>>
{

}
