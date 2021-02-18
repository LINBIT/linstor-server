package com.linbit.linstor.api.pojo.backups;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class RscDfnMetaPojo
{
    private final Map<String, String> props;
    private final long flags;
    private final Map<Integer, VlmDfnMetaPojo> vlmDfns;

    public RscDfnMetaPojo(Map<String, String> propsRef, long flagsRef, Map<Integer, VlmDfnMetaPojo> vlmDfnsRef)
    {
        props = propsRef;
        flags = flagsRef;
        vlmDfns = vlmDfnsRef;
    }

    public Map<String, String> getProps()
    {
        return props;
    }

    public long getFlags()
    {
        return flags;
    }

    public Map<Integer, VlmDfnMetaPojo> getVlmDfns()
    {
        return vlmDfns;
    }
}
