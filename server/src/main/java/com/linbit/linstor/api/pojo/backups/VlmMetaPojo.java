package com.linbit.linstor.api.pojo.backups;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class VlmMetaPojo
{
    private final Map<String, String> props;
    private final long flags;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public VlmMetaPojo(@JsonProperty("props") Map<String, String> propsRef, @JsonProperty("flags") long flagsRef)
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
