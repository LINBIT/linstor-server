package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.utils.Pair;

import java.util.Map;
import java.util.Set;

public interface DrbdLayerCtrlDatabaseDriver extends DrbdLayerDatabaseDriver
{
    void loadLayerData(
        Map<ResourceName, ResourceDefinition> tmpRscDfnMapRef,
        Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> tmpSnapDfnMapRef
    )
        throws DatabaseException;

    void clearLoadCache();

    <RSC extends AbsResource<RSC>> Pair<DrbdRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC rscRef,
        int idRef,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> tmpStorPoolMapRef
    )
        throws DatabaseException;

    default String getId(DrbdVlmData<?> drbdVlmData)
    {
        return "(LayerRscId=" + drbdVlmData.getRscLayerId() +
            ", VlmNr=" + drbdVlmData.getVlmNr() + ")";
    }

    default String getId(DrbdRscData<?> drbdRscData)
    {
        return "(LayerRscId=" + drbdRscData.getRscLayerId() + ")";
    }

    default String getId(DrbdRscDfnData<?> drbdRscDfnData)
    {
        return "(ResName=" + drbdRscDfnData.getResourceName() +
            ", ResNameSuffix=" + drbdRscDfnData.getRscNameSuffix() +
            ", SnapName=" + drbdRscDfnData.getSnapshotName() + ")";
    }

    default String getId(DrbdVlmDfnData<?> drbdVlmDfnData)
    {
        return "(ResName=" + drbdVlmDfnData.getResourceName() +
            ", ResNameSuffix=" + drbdVlmDfnData.getRscNameSuffix() +
            ", SnapName=" + drbdVlmDfnData.getSnapshotName() +
            ", VlmNr=" + drbdVlmDfnData.getVolumeNumber().value + ")";
    }
}
