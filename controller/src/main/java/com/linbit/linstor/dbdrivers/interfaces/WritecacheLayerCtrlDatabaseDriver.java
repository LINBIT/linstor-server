package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.utils.Pair;

import java.util.Map;
import java.util.Set;

public interface WritecacheLayerCtrlDatabaseDriver extends WritecacheLayerDatabaseDriver
{
    Pair<? extends RscLayerObject, Set<RscLayerObject>> load(
        Resource rscRef,
        int idRef,
        String rscSuffixRef,
        RscLayerObject parentRef,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> tmpStorPoolMapRef
    )
        throws DatabaseException;
}
