package com.linbit.drbdmanage.storage;

public class VolumeInfo
{
    private long size;
    private String identifier;
    private String path;

    public VolumeInfo(final long size, final String identifier, final String path)
    {
        super();
        this.size = size;
        this.identifier = identifier;
        this.path = path;
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
