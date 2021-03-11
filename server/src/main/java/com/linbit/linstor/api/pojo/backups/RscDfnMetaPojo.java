package com.linbit.linstor.api.pojo.backups;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RscDfnMetaPojo
{
    private final Map<String, String> props;
    private final long flags;
    private final Map<Integer, VlmDfnMetaPojo> vlmDfns;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public RscDfnMetaPojo(
        @JsonProperty("props") Map<String, String> propsRef,
        @JsonProperty("flags") long flagsRef,
        @JsonProperty("vlmDfns") Map<Integer, VlmDfnMetaPojo> vlmDfnsRef
    )
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
