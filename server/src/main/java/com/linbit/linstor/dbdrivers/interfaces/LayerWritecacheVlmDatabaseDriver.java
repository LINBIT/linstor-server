package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;

public interface LayerWritecacheVlmDatabaseDriver extends AbsLayerDataDatabaseDriver<WritecacheVlmData<?>>
{
    // WritecacheVlmData do have an additional storage pool, but that cannot be changed, so no special driver for that
}
