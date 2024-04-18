package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.MapDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteRscGrpDriver
    extends AbsSatelliteDbDriver<ResourceGroup>
    implements ResourceGroupDatabaseDriver
{
    private final SingleColumnDatabaseDriver<ResourceGroup, String> descriptionDriver;
    private final CollectionDatabaseDriver<ResourceGroup, DeviceLayerKind> layerStackDriver;
    private final SingleColumnDatabaseDriver<ResourceGroup, Integer> replicaCountDriver;
    private final CollectionDatabaseDriver<ResourceGroup, String> nodeNameDriver;
    private final CollectionDatabaseDriver<ResourceGroup, String> storPoolNameDriver;
    private final CollectionDatabaseDriver<ResourceGroup, String> storPoolDisklessNameDriver;
    private final CollectionDatabaseDriver<ResourceGroup, String> doNotPlaceWithRscListDriver;
    private final SingleColumnDatabaseDriver<ResourceGroup, String> doNotPlaceWithRscRegexDriver;
    private final CollectionDatabaseDriver<ResourceGroup, String> replicasOnSameListDriver;
    private final CollectionDatabaseDriver<ResourceGroup, String> replicasOnDifferentDriver;
    private final MapDatabaseDriver<ResourceGroup, String, Integer> xReplicasOnDifferentDriver;
    private final CollectionDatabaseDriver<ResourceGroup, DeviceProviderKind> allowedProviderListDriver;
    private final SingleColumnDatabaseDriver<ResourceGroup, Boolean> disklessOnRemainingDriver;
    private final SingleColumnDatabaseDriver<ResourceGroup, Short> peerSlotsDriver;

    @Inject
    public SatelliteRscGrpDriver()
    {
        descriptionDriver = getNoopColumnDriver();
        layerStackDriver = getNoopCollectionDriver();
        replicaCountDriver = getNoopColumnDriver();
        nodeNameDriver = getNoopCollectionDriver();
        storPoolNameDriver = getNoopCollectionDriver();
        storPoolDisklessNameDriver = getNoopCollectionDriver();
        doNotPlaceWithRscListDriver = getNoopCollectionDriver();
        doNotPlaceWithRscRegexDriver = getNoopColumnDriver();
        replicasOnSameListDriver = getNoopCollectionDriver();
        replicasOnDifferentDriver = getNoopCollectionDriver();
        xReplicasOnDifferentDriver = getNoopMapDriver();
        allowedProviderListDriver = getNoopCollectionDriver();
        disklessOnRemainingDriver = getNoopColumnDriver();
        peerSlotsDriver = getNoopColumnDriver();
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceGroup, String> getDescriptionDriver()
    {
        return descriptionDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroup, DeviceLayerKind> getLayerStackDriver()
    {
        return layerStackDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceGroup, Integer> getReplicaCountDriver()
    {
        return replicaCountDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getNodeNameDriver()
    {
        return nodeNameDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getStorPoolNameDriver()
    {
        return storPoolNameDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getStorPoolDisklessNameDriver()
    {
        return storPoolDisklessNameDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getDoNotPlaceWithRscListDriver()
    {
        return doNotPlaceWithRscListDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceGroup, String> getDoNotPlaceWithRscRegexDriver()
    {
        return doNotPlaceWithRscRegexDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getReplicasOnSameListDriver()
    {
        return replicasOnSameListDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getReplicasOnDifferentDriver()
    {
        return replicasOnDifferentDriver;
    }

    @Override
    public MapDatabaseDriver<ResourceGroup, String, Integer> getXReplicasOnDifferentMapDriver()
    {
        return xReplicasOnDifferentDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceGroup, DeviceProviderKind> getAllowedProviderListDriver()
    {
        return allowedProviderListDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceGroup, Boolean> getDisklessOnRemainingDriver()
    {
        return disklessOnRemainingDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceGroup, Short> getPeerSlotsDriver()
    {
        return peerSlotsDriver;
    }
}
