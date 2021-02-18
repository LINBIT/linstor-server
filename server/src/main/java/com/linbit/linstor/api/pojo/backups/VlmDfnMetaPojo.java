package com.linbit.linstor.api.pojo.backups;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class VlmDfnMetaPojo
{
    private final Map<String, String> props;
    private final long flags;
    private final long size;

    public VlmDfnMetaPojo(Map<String, String> propsRef, long flagsRef, long sizeRef)
    {
        props = propsRef;
        flags = flagsRef;
        size = sizeRef;
    }

    public Map<String, String> getProps()
    {
        return props;
    }

    public long getFlags()
    {
        return flags;
    }

    public long getSize()
    {
        return size;
    }
}
