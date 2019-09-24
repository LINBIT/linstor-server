package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

import java.util.Map;

public interface VolumeGroupCtrlDatabaseDriver extends VolumeGroupDatabaseDriver,
    ControllerDatabaseDriver<VolumeGroup, Void, Map<ResourceGroupName, ? extends ResourceGroup>>
{

}
