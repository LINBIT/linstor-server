package com.linbit.linstor.api.pojo.backups;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class RscMetaPojo
{
    private final Map<String, String> props;
    private final long flags;
    private final Map<Integer, VlmMetaPojo> vlms;

    public RscMetaPojo(Map<String, String> propsRef, long flagsRef, Map<Integer, VlmMetaPojo> vlmsRef)
    {
        props = propsRef;
        flags = flagsRef;
        vlms = vlmsRef;
    }

    public Map<String, String> getProps()
    {
        return props;
    }

    public long getFlags()
    {
        return flags;
    }

    public Map<Integer, VlmMetaPojo> getVlms()
    {
        return vlms;
    }
}
