package com.linbit.linstor.api.pojo.backups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class BackupMetaInfoPojo
{
    private final String name;
    private final long finishedTimestamp;
    private final String node;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public BackupMetaInfoPojo(
        @JsonProperty("name") String nameRef,
        @JsonProperty("finishedTimestamp") long finishedTimestampRef,
        @JsonProperty("node") String nodeRef
    )
    {
        name = nameRef;
        finishedTimestamp = finishedTimestampRef;
        node = nodeRef;
    }

    public String getName()
    {
        return name;
    }

    public long getFinishedTimestamp()
    {
        return finishedTimestamp;
    }

    public String getNode()
    {
        return node;
    }
}
