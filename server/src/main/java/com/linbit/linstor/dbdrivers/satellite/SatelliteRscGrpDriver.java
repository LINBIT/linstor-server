package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.CollectionDatabaseDriver;
import com.linbit.NoOpCollectionDatabaseDriver;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.ResourceGroupData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDataDatabaseDriver;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteRscGrpDriver implements ResourceGroupDataDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();
    private final CollectionDatabaseDriver<?, ?> noopColDriver = new NoOpCollectionDatabaseDriver<>();

    @Inject
    public SatelliteRscGrpDriver()
    {
    }

    @Override
    public void create(ResourceGroupData rscGrpRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(ResourceGroupData rscGrpRef) throws DatabaseException
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ResourceGroupData, String> getDescriptionDriver()
    {
        return (SingleColumnDatabaseDriver<ResourceGroupData, String>) noopSingleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceGroupData, DeviceLayerKind> getLayerStackDriver()
    {
        return (CollectionDatabaseDriver<ResourceGroupData, DeviceLayerKind>) noopColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ResourceGroupData, Integer> getReplicaCountDriver()
    {
        return (SingleColumnDatabaseDriver<ResourceGroupData, Integer>) noopSingleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ResourceGroupData, String> getStorPoolNameDriver()
    {
        return (SingleColumnDatabaseDriver<ResourceGroupData, String>) noopSingleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceGroupData, String> getDoNotPlaceWithRscListDriver()
    {
        return (CollectionDatabaseDriver<ResourceGroupData, String>) noopColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ResourceGroupData, String> getDoNotPlaceWithRscRegexDriver()
    {
        return (SingleColumnDatabaseDriver<ResourceGroupData, String>) noopSingleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceGroupData, String> getReplicasOnSameListDriver()
    {
        return (CollectionDatabaseDriver<ResourceGroupData, String>) noopColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceGroupData, String> getReplicasOnDifferentDriver()
    {
        return (CollectionDatabaseDriver<ResourceGroupData, String>) noopColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceGroupData, DeviceProviderKind> getAllowedProviderListDriver()
    {
        return (CollectionDatabaseDriver<ResourceGroupData, DeviceProviderKind>) noopColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ResourceGroupData, Boolean> getDisklessOnRemainingDriver()
    {
        return (SingleColumnDatabaseDriver<ResourceGroupData, Boolean>) noopSingleColDriver;
    }
}
