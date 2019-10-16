package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeConnection;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.utils.Triple;

import java.util.Map;

public interface VolumeConnectionCtrlDatabaseDriver extends VolumeConnectionDatabaseDriver,
    ControllerDatabaseDriver<VolumeConnection,
        Void,
        Map<Triple<NodeName, ResourceName, VolumeNumber>, Volume>>
{

}
