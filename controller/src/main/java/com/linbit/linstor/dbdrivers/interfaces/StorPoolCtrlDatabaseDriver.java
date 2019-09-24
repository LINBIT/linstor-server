package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.FreeSpaceMgrName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.utils.Pair;

import java.util.Map;

public interface StorPoolCtrlDatabaseDriver extends StorPoolDatabaseDriver,
    ControllerDatabaseDriver<StorPool,
        StorPool.InitMaps,
        Pair<Map<NodeName, ? extends Node>,
            Map<StorPoolName, ? extends StorPoolDefinition>>>
{

    Map<FreeSpaceMgrName, FreeSpaceMgr> getAllLoadedFreeSpaceMgrs();
}
