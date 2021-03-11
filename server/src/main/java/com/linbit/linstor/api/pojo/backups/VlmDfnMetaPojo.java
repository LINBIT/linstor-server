package com.linbit.linstor.api.pojo.backups;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class VlmDfnMetaPojo
{
    private final Map<String, String> props;
    private final long flags;
    private final long size;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public VlmDfnMetaPojo(
        @JsonProperty("props") Map<String, String> propsRef,
        @JsonProperty("flags") long flagsRef,
        @JsonProperty("size") long sizeRef
    )
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
