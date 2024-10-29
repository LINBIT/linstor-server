package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.apis.SnapshotVolumeDefinitionApi;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class SnapshotVlmDfnPojo implements SnapshotVolumeDefinitionApi
{
    private final UUID uuid;
    private final Integer volumeNr;
    private final long size;
    private final long flags;
    private final Map<String, String> snapVlmDfnPropsMap;
    private final Map<String, String> vlmDfnPropsMap;

    public SnapshotVlmDfnPojo(
        UUID uuidRef,
        Integer volumeNrRef,
        long sizeRef,
        long flagsRef,
        Map<String, String> snapVlmDfnPropsMapRef,
        Map<String, String> vlmDfnPropsMapRef
    )
    {
        uuid = uuidRef;
        volumeNr = volumeNrRef;
        size = sizeRef;
        flags = flagsRef;
        snapVlmDfnPropsMap = Collections.unmodifiableMap(snapVlmDfnPropsMapRef);
        vlmDfnPropsMap = Collections.unmodifiableMap(vlmDfnPropsMapRef);
    }

    @Override
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public Integer getVolumeNr()
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
    public Map<String, String> getSnapVlmDfnPropsMap()
    {
        return snapVlmDfnPropsMap;
    }

    @Override
    public Map<String, String> getVlmDfnPropsMap()
    {
        return vlmDfnPropsMap;
    }
}
