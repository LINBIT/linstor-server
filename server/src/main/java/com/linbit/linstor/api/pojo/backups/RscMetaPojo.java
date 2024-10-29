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
public class RscMetaPojo
{
    private final Map<String, String> snapProps;
    private final Map<String, String> rscProps;
    private final long flags;
    private final Map<Integer, VlmMetaPojo> vlms;

    public RscMetaPojo(
        Map<String, String> snapPropsRef,
        Map<String, String> rscPropsRef,
        long flagsRef,
        Map<Integer, VlmMetaPojo> vlmsRef
    )
    {
        this(null, snapPropsRef, rscPropsRef, flagsRef, vlmsRef);
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public RscMetaPojo(
        @JsonProperty("props") @Deprecated @Nullable Map<String, String> propsRef,
        @JsonProperty("snapProps") @Nullable Map<String, String> snapPropsRef,
        @JsonProperty("rscProps") @Nullable Map<String, String> rscPropsRef,
        @JsonProperty("flags") long flagsRef,
        @JsonProperty("vlms") Map<Integer, VlmMetaPojo> vlmsRef
    )
    {
        flags = flagsRef;
        vlms = vlmsRef;

        if (propsRef != null)
        {
            snapProps = new TreeMap<>();
            rscProps = new TreeMap<>();
            MigrationUtils_SplitSnapProps.splitS3Props(InstanceType.SNAP, propsRef, snapProps, rscProps);
        }
        else
        {
            snapProps = snapPropsRef;
            rscProps = rscPropsRef;
        }
    }

    public Map<String, String> getSnapProps()
    {
        return snapProps;
    }

    public Map<String, String> getRscProps()
    {
        return rscProps;
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
