package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.ImplementationError;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.interfaces.NvmeLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.utils.Pair;

import javax.inject.Inject;

import java.util.Set;

public class SatelliteNvmeLayerDriver implements NvmeLayerDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();
    private final ResourceLayerIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteResourceLayerIdDriver();

    @Inject
    public SatelliteNvmeLayerDriver()
    {
    }


    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public void create(NvmeRscData nvmeRscDataRef)
    {
        // no-op
    }

    @Override
    public void delete(NvmeRscData nvmeRscDataRef)
    {
        // no-op
    }

    @Override
    public void persist(NvmeVlmData nvmeVlmDataRef)
    {
        // no-op
    }

    @Override
    public void delete(NvmeVlmData nvmeVlmDataRef)
    {
        // no-op
    }

    @Override
    public Pair<? extends RscLayerObject,
        Set<RscLayerObject>> load(Resource rscRef, int idRef, String rscSuffixRef, RscLayerObject parentRef)
    {
        throw new ImplementationError("This method should never be called on satellite");
    }
}
