package com.linbit.linstor.api.pojo;

public class FilePojo
{
    public final String fileName;
    public final long timestamp;
    public final byte[] content;
    public final long offset;

    public FilePojo(String relativeNameRef, long timestampRef, byte[] contentRef, long offsetRef)
    {
        fileName = relativeNameRef;
        timestamp = timestampRef;
        content = contentRef;
        offset = offsetRef;
    }
}
