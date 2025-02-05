package com.linbit.linstor.api.pojo.backups;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.migration.MigrationUtils_SplitSnapProps;
import com.linbit.linstor.dbdrivers.migration.MigrationUtils_SplitSnapProps.InstanceType;

import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class VlmDfnMetaPojo
{
    private final Map<String, String> snapVlmDfnProps;
    private final Map<String, String> vlmDfnProps;
    private final long flags;
    private final long size;

    public VlmDfnMetaPojo(
        Map<String, String> snapVlmDfnPropsRef,
        Map<String, String> vlmDfnPropsRef,
        long flagsRef,
        long sizeRef
    )
    {
        this(null, snapVlmDfnPropsRef, vlmDfnPropsRef, flagsRef, sizeRef);
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public VlmDfnMetaPojo(
        @JsonProperty("props") @Deprecated @Nullable Map<String, String> propsRef,
        @JsonProperty("snapVlmDfnProps") @Nullable Map<String, String> snapVlmDfnPropsRef,
        @JsonProperty("vlmDfnProps") @Nullable Map<String, String> vlmDfnPropsRef,
        @JsonProperty("flags") long flagsRef,
        @JsonProperty("size") long sizeRef
    )
    {
        flags = flagsRef;
        size = sizeRef;

        if (propsRef != null)
        {
            snapVlmDfnProps = new TreeMap<>();
            vlmDfnProps = new TreeMap<>();
            MigrationUtils_SplitSnapProps.splitS3Props(
                InstanceType.SNAP_VLM_DFN,
                propsRef,
                snapVlmDfnProps,
                vlmDfnProps
            );
        }
        else
        {
            snapVlmDfnProps = snapVlmDfnPropsRef;
            vlmDfnProps = vlmDfnPropsRef;
        }
    }

    public Map<String, String> getSnapVlmDfnProps()
    {
        return snapVlmDfnProps;
    }

    public Map<String, String> getVlmDfnProps()
    {
        return vlmDfnProps;
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
