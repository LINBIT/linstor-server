package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import java.util.Map;

public interface SnapshotVolumeDefinitionCtrlDatabaseDriver extends SnapshotVolumeDefinitionDatabaseDriver,
    ControllerDatabaseDriver<SnapshotVolumeDefinition,
        SnapshotVolumeDefinition.InitMaps,
        PairNonNull<
            Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition>,
            Map<Pair<ResourceName, VolumeNumber>, VolumeDefinition>>>
{

}
