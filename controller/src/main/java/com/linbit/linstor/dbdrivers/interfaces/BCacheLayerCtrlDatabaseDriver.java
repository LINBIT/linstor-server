package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.utils.Pair;

import java.util.Map;
import java.util.Set;

public interface BCacheLayerCtrlDatabaseDriver extends BCacheLayerDatabaseDriver
{
    <RSC extends AbsResource<RSC>> Pair<? extends BCacheRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC rscRef,
        int idRef,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> tmpStorPoolMapRef
    )
        throws DatabaseException;

    default String getId(BCacheVlmData<?> bcacheVlmData)
    {
        return "(LayerRscId=" + bcacheVlmData.getRscLayerId() +
            ", SuffResName=" + bcacheVlmData.getRscLayerObject().getSuffixedResourceName() +
            ", VlmNr=" + bcacheVlmData.getVlmNr().value +
            ")";
    }
}
