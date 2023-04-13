package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;

public interface LayerLuksVlmDatabaseDriver extends AbsLayerDataDatabaseDriver<LuksVlmData<?>>
{
    SingleColumnDatabaseDriver<LuksVlmData<?>, byte[]> getVlmEncryptedPasswordDriver();
}
