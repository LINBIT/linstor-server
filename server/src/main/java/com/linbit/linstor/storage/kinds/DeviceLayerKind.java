package com.linbit.linstor.storage.kinds;

import java.util.List;

public enum DeviceLayerKind
{
    DRBD(
        false,
        true,
        ExtTools.DRBD9_KERNEL,
        ExtTools.DRBD9_UTILS
    ),
    //    DRBD_PROXY(
    //        false,
    //        true,
    //        false,
    //        false,
    //        StartupVerifications.DRBD_PROXY
    //    ),
    LUKS(
        true,
        true,
        ExtTools.CRYPT_SETUP
    ),
    NVME(
        false,
        true,
        ExtTools.NVME
    ),
    WRITECACHE(
        true,
        true,
        ExtTools.DM_WRITECACHE
    ),
    CACHE(
        true,
        true,
        ExtTools.DM_CACHE
    ),
    BCACHE(
        true,
        true,
        ExtTools.BCACHE_TOOLS
    ),
    STORAGE(true, true);
    private final ExtTools[] startupVerifications;

    private boolean localOnly;
    private boolean isShrinkingSupported;

    DeviceLayerKind(
        boolean localOnlyRef,
        boolean isShrinkingSupportedRef,
        ExtTools... startupVerificationsRef
    )
    {
        isShrinkingSupported = isShrinkingSupportedRef;
        startupVerifications = startupVerificationsRef;
        localOnly = localOnlyRef;
    }

    public ExtTools[] getExtToolDependencies()
    {
        return startupVerifications;
    }

    public boolean isLocalOnly()
    {
        return localOnly;
    }

    public boolean isShrinkingSupported()
    {
        return isShrinkingSupported;
    }

    public boolean isAncestorOf(List<DeviceLayerKind> layerListRef, DeviceLayerKind otherRef)
    {
        int localIdx = layerListRef.indexOf(this);
        int otherIdx = layerListRef.indexOf(otherRef);
        return localIdx < otherIdx;
    }
}
