package com.linbit.linstor.api.pojo.backups;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RscMetaPojo
{
    private final Map<String, String> props;
    private final long flags;
    private final Map<Integer, VlmMetaPojo> vlms;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public RscMetaPojo(
        @JsonProperty("props") Map<String, String> propsRef,
        @JsonProperty("flags") long flagsRef,
        @JsonProperty("vlms") Map<Integer, VlmMetaPojo> vlmsRef
    )
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
