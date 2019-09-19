package com.linbit.linstor.storage.layer.provider.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

public class StltProviderUtils
{
    public static long getAllocatedSize(VlmProviderObject vlmData, ExtCmd extCmd)
        throws StorageException
    {
        long size;
        if (vlmData.exists())
        {
            size = Commands.getBlockSizeInKib(extCmd, vlmData.getDevicePath());
        }
        else
        {
            throw new StorageException(
                "Device does not exist.",
                "Device does not exist.",
                "The volume could not be found on the system.",
                null,
                null
            );
        }
        return size;
    }
}
