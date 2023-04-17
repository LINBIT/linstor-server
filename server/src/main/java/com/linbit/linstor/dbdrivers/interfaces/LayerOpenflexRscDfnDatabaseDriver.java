package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscDfnData;

public interface LayerOpenflexRscDfnDatabaseDriver extends GenericDatabaseDriver<OpenflexRscDfnData<?>>
{
    SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String> getNqnDriver() throws DatabaseException;
}
