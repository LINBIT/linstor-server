package com.linbit.linstor.api.pojo;

public class FileInfoPojo
{
    public final String relativeName;
    public final long size;

    public FileInfoPojo(String relativeNameRef, long sizeRef)
    {
        relativeName = relativeNameRef;
        size = sizeRef;
    }
}
