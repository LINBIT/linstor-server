package com.linbit.linstor.api.pojo.backups;

import javax.annotation.Nullable;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class LayerMetaPojo
{
    private final String type;
    private final String rscNameSuffix;
    private final DrbdLayerMetaPojo drbd;
    private final LuksLayerMetaPojo luks;
    private final LayerMetaPojo cache;
    private final LayerMetaPojo writecache;
    private final StorageLayerMetaPojo storage;
    private final LayerMetaPojo nvme;
    private final List<LayerMetaPojo> children;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public LayerMetaPojo(
        @JsonProperty("type") String typeRef,
        @JsonProperty("rscNameSuffix") String rscNameSuffixRef,
        @JsonProperty("drbd") @Nullable DrbdLayerMetaPojo drbdRef,
        @JsonProperty("luks") @Nullable LuksLayerMetaPojo luksRef,
        @JsonProperty("cache") @Nullable LayerMetaPojo cacheRef,
        @JsonProperty("writecache") @Nullable LayerMetaPojo writecacheRef,
        @JsonProperty("storage") @Nullable StorageLayerMetaPojo storageRef,
        @JsonProperty("nvme") @Nullable LayerMetaPojo nvmeRef,
        @JsonProperty("children") @Nullable List<LayerMetaPojo> childrenRef
    )
    {
        type = typeRef;
        rscNameSuffix = rscNameSuffixRef;
        drbd = drbdRef;
        luks = luksRef;
        cache = cacheRef;
        writecache = writecacheRef;
        storage = storageRef;
        nvme = nvmeRef;
        children = childrenRef;
    }

    public String getType()
    {
        return type;
    }

    public DrbdLayerMetaPojo getDrbd()
    {
        return drbd;
    }

    public LuksLayerMetaPojo getLuks()
    {
        return luks;
    }

    public List<LayerMetaPojo> getChildren()
    {
        return children;
    }

    public LayerMetaPojo getCache()
    {
        return cache;
    }

    public LayerMetaPojo getWritecache()
    {
        return writecache;
    }

    public StorageLayerMetaPojo getStorage()
    {
        return storage;
    }

    public LayerMetaPojo getNvme()
    {
        return nvme;
    }

    public String getRscNameSuffix()
    {
        return rscNameSuffix;
    }
}
