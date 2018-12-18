package com.linbit.linstor.storage.layer.provider.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.Volume;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;

public class ProviderUtils
{
    public static void updateAllocatedSize(Volume vlm, ExtCmd extCmd, AccessContext accCtx)
        throws StorageException, AccessDeniedException
    {
        vlm.setAllocatedSize(
            accCtx,
            Commands.getBlockSizeInKib(
                extCmd,
                vlm.getDevicePath(accCtx)
            )
        );
    }

    public static long getAllocatedSize(Volume vlm, ExtCmd extCmd, AccessContext accCtx)
        throws StorageException, AccessDeniedException
    {
        return Commands.getBlockSizeInKib(extCmd, vlm.getDevicePath(accCtx));
    }
}
