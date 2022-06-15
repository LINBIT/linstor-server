package com.linbit.linstor.api.pojo;

public class FileInfoPojo
{
    public final String fileName;
    public final long size;
    public final long timestamp;

    public FileInfoPojo(String fileNameRef, long sizeRef, long timestampRef)
    {
        fileName = fileNameRef;
        size = sizeRef;
        timestamp = timestampRef;
    }
}
