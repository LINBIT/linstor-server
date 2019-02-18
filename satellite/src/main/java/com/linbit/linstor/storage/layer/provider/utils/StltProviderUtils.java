package com.linbit.linstor.storage.layer.provider.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;

public class StltProviderUtils
{
    public static long getAllocatedSize(VlmProviderObject vlmData, ExtCmd extCmd)
        throws StorageException
    {
        return Commands.getBlockSizeInKib(extCmd, vlmData.getDevicePath());
    }
}
