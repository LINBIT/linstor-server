package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerOpenflexRscDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscDfnData;

import javax.inject.Inject;

public class SatelliteLayerOpenflexRscDfnDbDriver implements LayerOpenflexRscDfnDatabaseDriver
{
    private final SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String> noopNqnDriver;

    @Inject
    public SatelliteLayerOpenflexRscDfnDbDriver()
    {
        noopNqnDriver = new SatelliteSingleColDriver<>();
    }

    @Override
    public void create(OpenflexRscDfnData<?> openflexRscDfnDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(OpenflexRscDfnData<?> openflexRscDfnDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String> getNqnDriver() throws DatabaseException
    {
        return noopNqnDriver;
    }
}

