package com.linbit.linstor.storage;

public class VolumeInfo
{
    private long size;
    private String identifier;
    private String path;

    public VolumeInfo(final long vlmSize, final String vlmIdentifier, final String vlmPath)
    {
        super();
        size = vlmSize;
        identifier = vlmIdentifier;
        path = vlmPath;
    }

    public long getSize()
    {
        return size;
    }

    public String getIdentifier()
    {
        return identifier;
    }

    public String getPath()
    {
        return path;
    }
}
