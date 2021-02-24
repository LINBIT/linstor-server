package com.linbit.linstor.api.pojo.backups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class BackupInfoPojo
{
    private final String name;
    private final String finishedTime;
    private final String node;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public BackupInfoPojo(
        @JsonProperty("name") String nameRef,
        @JsonProperty("finishedTime") String finishedTimeRef,
        @JsonProperty("node") String nodeRef
    )
    {
        name = nameRef;
        finishedTime = finishedTimeRef;
        node = nodeRef;
    }

    public String getName()
    {
        return name;
    }

    public String getFinishedTime()
    {
        return finishedTime;
    }

    public String getNode()
    {
        return node;
    }
}
