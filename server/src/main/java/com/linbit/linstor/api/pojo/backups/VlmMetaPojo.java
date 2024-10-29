package com.linbit.linstor.api.pojo.backups;

import com.linbit.linstor.dbdrivers.migration.MigrationUtils_SplitSnapProps;
import com.linbit.linstor.dbdrivers.migration.MigrationUtils_SplitSnapProps.InstanceType;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class VlmMetaPojo
{
    private final Map<String, String> snapVlmProps;
    private final Map<String, String> vlmProps;
    private final long flags;

    public VlmMetaPojo(
        Map<String, String> snapVlmPropsRef,
        Map<String, String> vlmPropsRef,
        long flagsRef
    )
    {
        this(null, snapVlmPropsRef, vlmPropsRef, flagsRef);
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public VlmMetaPojo(
        @JsonProperty("props") @Deprecated @Nullable Map<String, String> propsRef,
        @JsonProperty("snapVlmProps") @Nullable Map<String, String> snapVlmPropsRef,
        @JsonProperty("vlmProps") @Nullable Map<String, String> vlmPropsRef,
        @JsonProperty("flags") long flagsRef
    )
    {
        flags = flagsRef;

        if (propsRef != null)
        {
            snapVlmProps = new TreeMap<>();
            vlmProps = new TreeMap<>();
            MigrationUtils_SplitSnapProps.splitS3Props(InstanceType.SNAP_DFN, propsRef, snapVlmProps, vlmProps);
        }
        else
        {
            snapVlmProps = snapVlmPropsRef;
            vlmProps = vlmPropsRef;
        }
    }

    public Map<String, String> getSnapVlmProps()
    {
        return snapVlmProps;
    }

    public Map<String, String> getVlmProps()
    {
        return vlmProps;
    }

    public long getFlags()
    {
        return flags;
    }
}
