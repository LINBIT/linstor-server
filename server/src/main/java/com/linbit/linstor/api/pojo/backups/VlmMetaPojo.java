package com.linbit.linstor.api.pojo.backups;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class VlmMetaPojo
{
    private final Map<String, String> props;
    private final long flags;

    public VlmMetaPojo(Map<String, String> propsRef, long flagsRef)
    {
        props = propsRef;
        flags = flagsRef;
    }

    public Map<String, String> getProps()
    {
        return props;
    }

    public long getFlags()
    {
        return flags;
    }
}
