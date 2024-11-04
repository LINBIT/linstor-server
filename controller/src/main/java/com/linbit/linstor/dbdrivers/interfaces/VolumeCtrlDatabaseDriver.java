package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import java.util.Map;

public interface VolumeCtrlDatabaseDriver extends VolumeDatabaseDriver,
    ControllerDatabaseDriver<Volume,
        Volume.InitMaps,
        PairNonNull<Map<Pair<NodeName, ResourceName>, ? extends Resource>,
            Map<Pair<ResourceName, VolumeNumber>, ? extends VolumeDefinition>>>
{
}
