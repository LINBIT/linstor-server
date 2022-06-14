package com.linbit.linstor.api.pojo;

public class FileInfoPojo
{
    public final String relativeName;
    public final long size;
    public final long timestamp;

    public FileInfoPojo(String relativeNameRef, long sizeRef, long timestampRef)
    {
        relativeName = relativeNameRef;
        size = sizeRef;
        timestamp = timestampRef;
    }
}
