package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;

public interface LayerBCacheRscDatabaseDriver extends AbsLayerDataDatabaseDriver<BCacheRscData<?>>
{
    // Since bcache layer (currently) does not have any resource related data, we also do not need
    // interface methods for it.
    // That makes this interface a marker interface (for now)
}
