package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpCollectionDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;

public class SatelliteResDfnDriver implements ResourceDefinitionDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final CollectionDatabaseDriver<?, ?> noOpColDriver = new NoOpCollectionDatabaseDriver<>();
    private final CoreModule.ResourceDefinitionMap resDfnMap;

    @Inject
    public SatelliteResDfnDriver(CoreModule.ResourceDefinitionMap resDfnMapRef)
    {
        resDfnMap = resDfnMapRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<ResourceDefinition> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<ResourceDefinition>) stateFlagsDriver;
    }

    @Override
    public void create(ResourceDefinition resDfn)
    {
        // no-op
    }

    @Override
    public void delete(ResourceDefinition data)
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceDefinition, DeviceLayerKind> getLayerStackDriver()
    {
        return (CollectionDatabaseDriver<ResourceDefinition, DeviceLayerKind>) noOpColDriver;
    }
}
