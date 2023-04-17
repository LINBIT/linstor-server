package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscData;

public interface LayerOpenflexRscDatabaseDriver extends AbsLayerDataDatabaseDriver<OpenflexRscData<?>>
{
    // Since nvme (which is also used for OpenFlex) layer (currently) does not have any resource related data, we also
    // do not need interface methods for it.
    // That makes this interface a marker interface (for now)
}
