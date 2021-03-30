package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpCollectionDatabaseDriver;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteRscGrpDriver implements ResourceGroupDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();
    private final CollectionDatabaseDriver<?, ?> noopColDriver = new NoOpCollectionDatabaseDriver<>();

    @Inject
    public SatelliteRscGrpDriver()
    {
    }

    @Override
    public void create(ResourceGroup rscGrpRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(ResourceGroup rscGrpRef) throws DatabaseException
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ResourceGroup, String> getDescriptionDriver()
    {
        return (SingleColumnDatabaseDriver<ResourceGroup, String>) noopSingleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceGroup, DeviceLayerKind> getLayerStackDriver()
    {
        return (CollectionDatabaseDriver<ResourceGroup, DeviceLayerKind>) noopColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ResourceGroup, Integer> getReplicaCountDriver()
    {
        return (SingleColumnDatabaseDriver<ResourceGroup, Integer>) noopSingleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getNodeNameDriver()
    {
        return (CollectionDatabaseDriver<ResourceGroup, String>) noopColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getStorPoolNameDriver()
    {
        return (CollectionDatabaseDriver<ResourceGroup, String>) noopColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getStorPoolDisklessNameDriver()
    {
        return (CollectionDatabaseDriver<ResourceGroup, String>) noopColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getDoNotPlaceWithRscListDriver()
    {
        return (CollectionDatabaseDriver<ResourceGroup, String>) noopColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ResourceGroup, String> getDoNotPlaceWithRscRegexDriver()
    {
        return (SingleColumnDatabaseDriver<ResourceGroup, String>) noopSingleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getReplicasOnSameListDriver()
    {
        return (CollectionDatabaseDriver<ResourceGroup, String>) noopColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceGroup, String> getReplicasOnDifferentDriver()
    {
        return (CollectionDatabaseDriver<ResourceGroup, String>) noopColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceGroup, DeviceProviderKind> getAllowedProviderListDriver()
    {
        return (CollectionDatabaseDriver<ResourceGroup, DeviceProviderKind>) noopColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ResourceGroup, Boolean> getDisklessOnRemainingDriver()
    {
        return (SingleColumnDatabaseDriver<ResourceGroup, Boolean>) noopSingleColDriver;
    }
}
