package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteLayerResourceIdDriver
    extends AbsSatelliteDbDriver<AbsRscLayerObject<?>>
    implements LayerResourceIdDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> parentDriver;
    private final SingleColumnDatabaseDriver<?, ?> suspendedDriver;

    @Inject
    public SatelliteLayerResourceIdDriver()
    {
        parentDriver = getNoopColumnDriver();
        suspendedDriver = getNoopColumnDriver();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <RSC extends AbsResource<RSC>, T extends VlmProviderObject<RSC>>
    SingleColumnDatabaseDriver<AbsRscData<RSC, T>, AbsRscLayerObject<RSC>> getParentDriver()
    {
        return (SingleColumnDatabaseDriver<AbsRscData<RSC, T>, AbsRscLayerObject<RSC>>) parentDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <RSC extends AbsResource<RSC>, VLM_TYPE extends VlmProviderObject<RSC>>
    SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, Boolean> getSuspendDriver()
    {
        return (SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, Boolean>) suspendedDriver;
    }
}
