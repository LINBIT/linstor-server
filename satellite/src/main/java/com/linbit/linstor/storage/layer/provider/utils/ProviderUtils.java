package com.linbit.linstor.storage.layer.provider.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.Volume;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;

public class ProviderUtils
{
    public static void updateSize(Volume vlm, ExtCmd extCmd, AccessContext accCtx)
        throws StorageException, AccessDeniedException
    {
        setSize(
            vlm,
            Commands.getBlockSizeInKib(
                extCmd,
                vlm.getDevicePath(accCtx)
            ),
            accCtx
        );
    }

    public static void setSize(Volume vlm, long blockSizeInKib, AccessContext accCtx)
        throws AccessDeniedException
    {
        vlm.setAllocatedSize(accCtx, blockSizeInKib);
        vlm.setUsableSize(accCtx, blockSizeInKib);
    }
}
