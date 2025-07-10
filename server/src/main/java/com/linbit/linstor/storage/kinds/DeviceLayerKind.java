package com.linbit.linstor.storage.kinds;

import java.util.List;

public enum DeviceLayerKind
{
    DRBD(
        80,
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
        50,
        true,
        true,
        ExtTools.CRYPT_SETUP
    ),
    NVME(
        20,
        false,
        true,
        ExtTools.NVME
    ),
    WRITECACHE(
        60,
        true,
        true,
        ExtTools.DM_WRITECACHE
    ),
    CACHE(
        60,
        true,
        true,
        ExtTools.DM_CACHE
    ),
    BCACHE(
        60,
        true,
        true,
        ExtTools.BCACHE_TOOLS
    ),
    STORAGE(10, true, true);
    private final ExtTools[] startupVerifications;

    private int order;
    private boolean localOnly;
    private boolean isShrinkingSupported;

    DeviceLayerKind(
        int orderRef,
        boolean localOnlyRef,
        boolean isShrinkingSupportedRef,
        ExtTools... startupVerificationsRef
    )
    {
        order = orderRef;
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

    public int getOrder()
    {
        return order;
    }

    public boolean isAncestorOf(List<DeviceLayerKind> layerListRef, DeviceLayerKind otherRef)
    {
        int localIdx = layerListRef.indexOf(this);
        int otherIdx = layerListRef.indexOf(otherRef);
        return localIdx < otherIdx;
    }
}
