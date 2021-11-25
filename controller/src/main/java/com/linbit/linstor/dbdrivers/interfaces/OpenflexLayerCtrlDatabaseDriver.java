package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscDfnData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexVlmData;
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
    )
        throws DatabaseException;

    void clearLoadAllCache();

    default String getId(OpenflexRscDfnData<?> openflexRscDfnData)
    {
        return "(ResName=" + openflexRscDfnData.getResourceName() +
            ", ResNameSuffix=" + openflexRscDfnData.getRscNameSuffix() + ")";
    }

    default String getId(OpenflexVlmData<?> ofVlmData)
    {
        return "(LayerRscId=" + ofVlmData.getRscLayerId() +
            ", VlmNr=" + ofVlmData.getVlmNr() + ")";
    }

    default String getId(OpenflexRscData<?> ofRscData)
    {
        return "(LayerRscId=" + ofRscData.getRscLayerId() +
            ", SuffResName=" + ofRscData.getSuffixedResourceName() +
            ")";
    }
}
