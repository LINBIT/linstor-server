package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.utils.Pair;

import java.util.Map;

public interface SnapshotCtrlDatabaseDriver extends SnapshotDatabaseDriver,
    ControllerDatabaseDriver<Snapshot,
        Snapshot.InitMaps,
        Pair<Map<NodeName, Node>, Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition>>>
{

}
