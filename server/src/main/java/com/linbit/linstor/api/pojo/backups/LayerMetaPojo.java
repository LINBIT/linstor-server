package com.linbit.linstor.api.pojo.backups;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

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

    public LayerMetaPojo(
        String typeRef,
        String rscNameSuffixRef,
        DrbdLayerMetaPojo drbdRef,
        LuksLayerMetaPojo luksRef,
        LayerMetaPojo cacheRef,
        LayerMetaPojo writecacheRef,
        StorageLayerMetaPojo storageRef,
        LayerMetaPojo nvmeRef,
        List<LayerMetaPojo> childrenRef
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
