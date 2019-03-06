package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.CollectionDatabaseDriver;
import com.linbit.NoOpCollectionDatabaseDriver;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;

public class SatelliteResDfnDriver implements ResourceDefinitionDataDatabaseDriver
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
    public StateFlagsPersistence<ResourceDefinitionData> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<ResourceDefinitionData>) stateFlagsDriver;
    }

    @Override
    public void create(ResourceDefinitionData resDfn)
    {
        // no-op
    }

    @Override
    public boolean exists(ResourceName resourceName)
    {
        return resDfnMap.containsKey(resourceName);
    }

    @Override
    public void delete(ResourceDefinitionData data)
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public CollectionDatabaseDriver<ResourceDefinitionData, DeviceLayerKind> getLayerStackDriver()
    {
        return (CollectionDatabaseDriver<ResourceDefinitionData, DeviceLayerKind>) noOpColDriver;
    }
}
