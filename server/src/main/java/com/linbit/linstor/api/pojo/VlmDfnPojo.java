package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class VlmDfnPojo implements VolumeDefinitionApi
{
    private final @Nullable UUID uuid;
    private final @Nullable Integer volumeNr;
    private final long size;
    private final long flags;
    private final Map<String, String> props;
    private final List<Pair<String, VlmDfnLayerDataApi>> layerData;

    public VlmDfnPojo(
        final @Nullable UUID uuidRef,
        final @Nullable Integer volumeNrRef,
        final long sizeRef,
        final long flagsRef,
        Map<String, String> propsRef,
        final List<Pair<String, VlmDfnLayerDataApi>> layerDataRef
    )
    {
        uuid = uuidRef;
        volumeNr = volumeNrRef;
        size = sizeRef;
        flags = flagsRef;
        props = propsRef;
        layerData = layerDataRef;
    }

    @Override
    public @Nullable UUID getUuid()
    {
        return uuid;
    }

    @Override
    public @Nullable Integer getVolumeNr()
    {
        return volumeNr;
    }

    @Override
    public long getSize()
    {
        return size;
    }

    @Override
    public long getFlags()
    {
        return flags;
    }

    @Override
    public Map<String, String> getProps()
    {
        return props;
    }

    @Override
    public List<Pair<String, VlmDfnLayerDataApi>> getVlmDfnLayerData()
    {
        return layerData;
    }

}
