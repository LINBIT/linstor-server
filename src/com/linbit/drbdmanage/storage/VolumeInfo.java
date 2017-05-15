package com.linbit.drbdmanage.storage;

public class VolumeInfo
{
    public long size;
    public String identifier;
    public String path;

    public VolumeInfo(final long size, final String identifier, final String path)
    {
        super();
        this.size = size;
        this.identifier = identifier;
        this.path = path;
    }
}
