package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

public class StltProviderUtils
{
    public static long getAllocatedSize(VlmProviderObject<?> vlmData, ExtCmd extCmd)
        throws StorageException
    {
        long size;
        if (vlmData.exists())
        {
            size = getAllocatedSize(vlmData.getDevicePath(), extCmd);
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

    public static long getAllocatedSize(String blockdef, ExtCmd extCmd)
        throws StorageException
    {
        return Commands.getBlockSizeInKib(extCmd, blockdef);
    }

    private StltProviderUtils()
    {
    }
}
