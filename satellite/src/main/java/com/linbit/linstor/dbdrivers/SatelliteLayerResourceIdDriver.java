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
public class SatelliteLayerResourceIdDriver implements LayerResourceIdDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteLayerResourceIdDriver()
    {
    }

    @Override
    public void delete(AbsRscLayerObject<?> rscLayerObjectRef)
    {
        // no-op
    }

    @Override
    public void create(AbsRscLayerObject<?> dataRef) throws DatabaseException
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

    @Override
    public <RSC extends AbsResource<RSC>, VLM_TYPE extends VlmProviderObject<RSC>> SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, Boolean> getSuspendDriver()
    {
        return (SingleColumnDatabaseDriver<AbsRscData<RSC, VLM_TYPE>, Boolean>) noopSingleColDriver;
    }
}
