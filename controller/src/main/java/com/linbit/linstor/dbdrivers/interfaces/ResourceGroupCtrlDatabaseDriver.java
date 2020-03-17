package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

public interface ResourceGroupCtrlDatabaseDriver extends ResourceGroupDatabaseDriver,
    ControllerDatabaseDriver<ResourceGroup, ResourceGroup.InitMaps, Void>
{

}
