package com.linbit.linstor.api.pojo;

public class VlmUpdatePojo
{
    private final int volumeNumber;
    private final String blockDevicePath;
    private final String metaDiskPath;
    private final long realSize;

    public VlmUpdatePojo(
        int volumeNumberRef,
        String blockDevicePathRef,
        String metaDiskPathRef,
        long realSizeRef
    )
    {
        volumeNumber = volumeNumberRef;
        blockDevicePath = blockDevicePathRef;
        metaDiskPath = metaDiskPathRef;
        realSize = realSizeRef;
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

    public long getRealSize()
    {
        return realSize;
    }
}
