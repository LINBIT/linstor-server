package com.linbit.linstor.api.pojo;

public class RequestFilePojo
{
    public final String name;
    public final long offset;
    public final long length;

    public RequestFilePojo(String nameRef, long offsetRef, long lengthRef)
    {
        name = nameRef;
        offset = offsetRef;
        length = lengthRef;
    }
}
