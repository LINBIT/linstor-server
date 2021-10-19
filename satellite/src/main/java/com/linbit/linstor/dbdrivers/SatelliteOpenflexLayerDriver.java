package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.OpenflexLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscDfnData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexVlmData;

import javax.inject.Inject;

public class SatelliteOpenflexLayerDriver implements OpenflexLayerDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();
    private final ResourceLayerIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteResourceLayerIdDriver();

    @Inject
    public SatelliteOpenflexLayerDriver()
    {
    }


    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public void create(OpenflexRscData<?> ofRscDataRef)
    {
        // no-op
    }

    @Override
    public void delete(OpenflexRscData<?> ofRscDataRef)
    {
        // no-op
    }

    @Override
    public void persist(OpenflexVlmData<?> ofVlmDataRef)
    {
        // no-op
    }

    @Override
    public void delete(OpenflexVlmData<?> ofVlmDataRef)
    {
        // no-op
    }

    @Override
    public void create(OpenflexRscDfnData<?> ofRscDfnDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(OpenflexRscDfnData<?> ofRscDfnDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String> getNqnDriver() throws DatabaseException
    {
        return (SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String>) noopSingleColDriver;
    }

}
