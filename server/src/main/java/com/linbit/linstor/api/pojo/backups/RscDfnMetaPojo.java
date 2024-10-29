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
public class RscDfnMetaPojo
{
    private final Map<String, String> snapDfnProps;
    private final Map<String, String> rscDfnProps;
    private final long flags;
    private final Map<Integer, VlmDfnMetaPojo> vlmDfns;

    public RscDfnMetaPojo(
        Map<String, String> snapDfnPropsRef,
        Map<String, String> rscDfnPropsRef,
        long flagsRef,
        Map<Integer, VlmDfnMetaPojo> vlmDfnsRef
    )
    {
        this(null, snapDfnPropsRef, rscDfnPropsRef, flagsRef, vlmDfnsRef);
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public RscDfnMetaPojo(
        @JsonProperty("props") @Deprecated @Nullable Map<String, String> propsRef,
        @JsonProperty("snapDfnProps") @Nullable Map<String, String> snapDfnPropsRef,
        @JsonProperty("rscDfnProps") @Nullable Map<String, String> rscDfnPropsRef,
        @JsonProperty("flags") long flagsRef,
        @JsonProperty("vlmDfns") Map<Integer, VlmDfnMetaPojo> vlmDfnsRef
    )
    {
        flags = flagsRef;
        vlmDfns = vlmDfnsRef;

        if (propsRef != null)
        {
            snapDfnProps = new TreeMap<>();
            rscDfnProps = new TreeMap<>();
            MigrationUtils_SplitSnapProps.splitS3Props(InstanceType.SNAP_DFN, propsRef, snapDfnProps, rscDfnProps);
        }
        else
        {
            snapDfnProps = snapDfnPropsRef;
            rscDfnProps = rscDfnPropsRef;
        }
    }

    public Map<String, String> getSnapDfnProps()
    {
        return snapDfnProps;
    }

    public Map<String, String> getRscDfnProps()
    {
        return rscDfnProps;
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
