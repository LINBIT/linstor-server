package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteResDfnDriver
    extends AbsSatelliteDbDriver<ResourceDefinition>
    implements ResourceDefinitionDatabaseDriver
{
    private final StateFlagsPersistence<ResourceDefinition> stateFlagsDriver;
    private final CollectionDatabaseDriver<ResourceDefinition, DeviceLayerKind> layerStackDriver;
    private final SingleColumnDatabaseDriver<ResourceDefinition, ResourceGroup> rscGrpDriver;

    @Inject
    public SatelliteResDfnDriver()
    {
        stateFlagsDriver = getNoopFlagDriver();
        layerStackDriver = getNoopCollectionDriver();
        rscGrpDriver = getNoopColumnDriver();
    }

    @Override
    public StateFlagsPersistence<ResourceDefinition> getStateFlagsPersistence()
    {
        return stateFlagsDriver;
    }

    @Override
    public CollectionDatabaseDriver<ResourceDefinition, DeviceLayerKind> getLayerStackDriver()
    {
        return layerStackDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceDefinition, ResourceGroup> getRscGrpDriver()
    {
        return rscGrpDriver;
    }
}
