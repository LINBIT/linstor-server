package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class VlmPojo implements VolumeApi
{
    private final @Nullable UUID vlmDfnUuid;
    private final @Nullable UUID vlmUuid;
    private final @Nullable String devicePath;
    private final int vlmNr;
    private final long vlmFlags;
    private final Map<String, String> vlmProps;
    private final Optional<Long> allocated;
    private final Optional<Long> usableSize;
    private final @Nullable List<Pair<String, VlmLayerDataApi>> layerData;

    private final @Nullable String compatStorPoolName;
    private final @Nullable DeviceProviderKind compatDevProviderKind;
    private final @Nullable ApiCallRc reports;

    public VlmPojo(
        final @Nullable UUID vlmDfnUuidRef,
        final @Nullable UUID vlmUuidRef,
        final @Nullable String devicePathRef,
        final int vlmNrRef,
        final long vlmFlagsRef,
        final Map<String, String> vlmPropsRef,
        final Optional<Long> allocatedRef,
        final Optional<Long> usableSizeRef,
        final @Nullable List<Pair<String, VlmLayerDataApi>> layerDataRef,
        final @Nullable String compatStorPoolNameRef,
        final @Nullable DeviceProviderKind compatDevProviderKindRef,
        final @Nullable ApiCallRc reportsRef
    )
    {
        vlmDfnUuid = vlmDfnUuidRef;
        vlmUuid = vlmUuidRef;
        devicePath = devicePathRef;
        vlmNr = vlmNrRef;
        vlmFlags = vlmFlagsRef;
        vlmProps = vlmPropsRef;
        allocated = allocatedRef;
        usableSize = usableSizeRef;
        layerData = layerDataRef;
        compatStorPoolName = compatStorPoolNameRef;
        compatDevProviderKind = compatDevProviderKindRef;
        reports = reportsRef != null ? reportsRef : new ApiCallRcImpl();
    }

    @Override
    public UUID getVlmDfnUuid()
    {
        return vlmDfnUuid;
    }

    @Override
    public UUID getVlmUuid()
    {
        return vlmUuid;
    }

    @Override
    public String getDevicePath()
    {
        return devicePath;
    }

    @Override
    public int getVlmNr()
    {
        return vlmNr;
    }

    @Override
    public long getFlags()
    {
        return vlmFlags;
    }

    @Override
    public Map<String, String> getVlmProps()
    {
        return vlmProps;
    }

    @Override
    public Optional<Long> getAllocatedSize()
    {
        return allocated;
    }

    @Override
    public Optional<Long> getUsableSize()
    {
        return usableSize;
    }

    @Override
    public List<Pair<String, VlmLayerDataApi>> getVlmLayerData()
    {
        return layerData;
    }

    @Override
    public String getStorPoolName()
    {
        return compatStorPoolName;
    }

    @Override
    public DeviceProviderKind getStorPoolDeviceProviderKind()
    {
        return compatDevProviderKind;
    }

    @Override
    public ApiCallRc getReports()
    {
        return reports;
    }
}
