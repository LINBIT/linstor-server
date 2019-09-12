package com.linbit.linstor.storage.kinds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DeviceProviderInfo
{
    private final DeviceProviderKind kind;
    private final boolean isSupported;

    private final Integer versionMajor;
    private final Integer versionMinor;
    private final Integer versionPatch;

    private final List<String> notSupportedReasons;

    public DeviceProviderInfo(
        DeviceProviderKind kindRef,
        boolean isSupportedRef,
        Integer versionMajorRef,
        Integer versionMinorRef,
        Integer versionPatchRef,
        List<String> notSupportedReasonsRef
    )
    {
        kind = kindRef;
        isSupported = isSupportedRef;
        versionMajor = versionMajorRef;
        versionMinor = versionMinorRef;
        versionPatch = versionPatchRef;
        notSupportedReasons = Collections.unmodifiableList(new ArrayList<>(notSupportedReasonsRef));
    }

    public final DeviceProviderKind getKind()
    {
        return kind;
    }

    public final boolean isSupported()
    {
        return isSupported;
    }

    public final Integer getVersionMajor()
    {
        return versionMajor;
    }

    public final Integer getVersionMinor()
    {
        return versionMinor;
    }

    public final Integer getVersionPatch()
    {
        return versionPatch;
    }

    public final List<String> getNotSupportedReasons()
    {
        return notSupportedReasons;
    }
}
