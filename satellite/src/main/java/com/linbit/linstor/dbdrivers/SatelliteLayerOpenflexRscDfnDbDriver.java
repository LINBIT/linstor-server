package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerOpenflexRscDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscDfnData;

import javax.inject.Inject;
import javax.inject.Singleton;


@Singleton
public class SatelliteLayerOpenflexRscDfnDbDriver
    extends AbsSatelliteDbDriver<OpenflexRscDfnData<?>>
    implements LayerOpenflexRscDfnDatabaseDriver
{
    private final SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String> nqnDriver;

    @Inject
    public SatelliteLayerOpenflexRscDfnDbDriver()
    {
        nqnDriver = getNoopColumnDriver();
    }

    @Override
    public SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String> getNqnDriver() throws DatabaseException
    {
        return nqnDriver;
    }
}
