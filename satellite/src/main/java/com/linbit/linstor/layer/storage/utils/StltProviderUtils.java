package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.utils.Commands;

public class StltProviderUtils
{
    public static long getAllocatedSize(VlmProviderObject<?> vlmData, ExtCmd extCmd)
        throws StorageException
    {
        long size;
        if (vlmData.exists())
        {
            String devicePath = vlmData.getDevicePath();
            if (devicePath != null)
            {
                size = getAllocatedSize(devicePath, extCmd);
            }
            else
            {
                throw new StorageException(
                    "DevicePath does not exist. VlmData: " + vlmData.getVolume() + ", suffix: " +
                        vlmData.getRscLayerObject().getResourceNameSuffix(),
                    "Device path is empty.",
                    "The volume could not be found on the system.",
                    null,
                    null
                );
            }
        }
        else
        {
            throw new StorageException(
                "Device does not exist. VlmData: " + vlmData.getVolume() + ", suffix: " +
                    vlmData.getRscLayerObject().getResourceNameSuffix(),
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
