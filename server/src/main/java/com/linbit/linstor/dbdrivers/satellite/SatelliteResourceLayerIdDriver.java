package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteResourceLayerIdDriver implements ResourceLayerIdDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteResourceLayerIdDriver()
    {
    }

    @Override
    public void delete(AbsRscLayerObject<?> rscLayerObjectRef)
    {
        // no-op
    }

    @Override
    public void persist(AbsRscLayerObject<?> rscLayerObjectRef)
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public<
        RSC extends AbsResource<RSC>,
        T extends VlmProviderObject<RSC>>
    SingleColumnDatabaseDriver<AbsRscData<RSC, T>, AbsRscLayerObject<RSC>> getParentDriver()
    {
        return (SingleColumnDatabaseDriver<AbsRscData<RSC, T>, AbsRscLayerObject<RSC>>) noopSingleColDriver;
    }
}
