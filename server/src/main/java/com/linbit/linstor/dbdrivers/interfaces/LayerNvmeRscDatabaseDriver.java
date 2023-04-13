package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;

public interface LayerNvmeRscDatabaseDriver extends AbsLayerDataDatabaseDriver<NvmeRscData<?>>
{
    // Since nvme layer (currently) does not have any resource related data, we also do not need
    // interface methods for it.
    // That makes this interface a marker interface (for now)
}
