package com.linbit.linstor.api.pojo;

import java.util.Map;

public class VlmUpdatePojo
{
    private final int volumeNumber;
    private final String blockDevicePath;
    private final String metaDiskPath;
    private final String devicePath;
    private final long usableSize;
    private long allocatedSize;
    private final Map<String, String> vlmDfnPropsMap;

    public VlmUpdatePojo(
        int volumeNumberRef,
        String blockDevicePathRef,
        String metaDiskPathRef,
        String devicePathRef,
        long usableSizeRef,
        long allocatedSizeRef,
        Map<String, String> vlmDfnPropsMapRef
    )
    {
        volumeNumber = volumeNumberRef;
        blockDevicePath = blockDevicePathRef;
        metaDiskPath = metaDiskPathRef;
        devicePath = devicePathRef;
        usableSize = usableSizeRef;
        allocatedSize = allocatedSizeRef;
        vlmDfnPropsMap = vlmDfnPropsMapRef;
    }

    public int getVolumeNumber()
    {
        return volumeNumber;
    }

    public String getBlockDevicePath()
    {
        return blockDevicePath;
    }

    public String getMetaDiskPath()
    {
        return metaDiskPath;
    }

    public String getDevicePath()
    {
        return devicePath;
    }

    public long getUsableSize()
    {
        return usableSize;
    }

    public Map<String, String> getVlmDfnPropsMap()
    {
        return vlmDfnPropsMap;
    }

    public long getAllocatedSize()
    {
        return allocatedSize;
    }
}
