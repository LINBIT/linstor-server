package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.utils.Pair;

import java.util.Map;
import java.util.Set;

public interface OpenflexLayerCtrlDatabaseDriver extends OpenflexLayerDatabaseDriver
{
    void fetchForLoadAll(
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> tmpStorPoolMapRef,
        Map<ResourceName, ResourceDefinition> rscDfnMap
    )
        throws DatabaseException;

    <RSC extends AbsResource<RSC>> Pair<? extends AbsRscLayerObject<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC rscRef,
        int idRef,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef
    );

    void clearLoadAllCache();
}
