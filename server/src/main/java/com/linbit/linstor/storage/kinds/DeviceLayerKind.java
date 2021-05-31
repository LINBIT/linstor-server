package com.linbit.linstor.storage.kinds;

public enum DeviceLayerKind
{
    DRBD(
        false,
        true,
        ExtTools.DRBD9
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
    OPENFLEX(
        false,
        true,
        ExtTools.NVME
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
}
