package com.linbit.linstor.api.pojo;

public class FilePojo
{
    public final String relativeName;
    public final long timestamp;
    public final byte[] content;
    public final long offset;

    public FilePojo(String relativeNameRef, long timestampRef, byte[] contentRef, long offsetRef)
    {
        relativeName = relativeNameRef;
        timestamp = timestampRef;
        content = contentRef;
        offset = offsetRef;
    }
}
