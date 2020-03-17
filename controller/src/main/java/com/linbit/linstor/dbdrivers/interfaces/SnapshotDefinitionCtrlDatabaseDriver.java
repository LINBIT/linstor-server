package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

import java.util.Map;

public interface SnapshotDefinitionCtrlDatabaseDriver extends SnapshotDefinitionDatabaseDriver,
    ControllerDatabaseDriver<SnapshotDefinition,
        SnapshotDefinition.InitMaps,
        Map<ResourceName, ResourceDefinition>>
{

}
