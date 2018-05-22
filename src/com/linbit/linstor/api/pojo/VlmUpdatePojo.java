package com.linbit.linstor.api.pojo;

public class VlmUpdatePojo
{
    private int volumeNumber;
    private String blockDevicePath;
    private String metaDiskPath;

    public VlmUpdatePojo(int volumeNumber, String blockDevicePath, String metaDiskPath)
    {
        this.volumeNumber = volumeNumber;
        this.blockDevicePath = blockDevicePath;
        this.metaDiskPath = metaDiskPath;
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
}
