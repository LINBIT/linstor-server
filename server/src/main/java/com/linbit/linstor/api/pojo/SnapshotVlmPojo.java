package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.apis.SnapshotVolumeApi;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class SnapshotVlmPojo implements SnapshotVolumeApi
{
    private final UUID snapshotVlmDfnUuid;
    private final UUID snapshotVlmUuid;
    private final int snapshotVlmNr;
    private final Map<String, String> snapVlmPropsMap;
    private final Map<String, String> vlmPropsMap;
    private final @Nullable String state;

    public SnapshotVlmPojo(
        final UUID snapshotVlmDfnUuidRef,
        final UUID snapshotVlmUuidRef,
        final int snapshotVlmNrRef,
        Map<String, String> snapVlmPropsMapRef,
        Map<String, String> vlmPropsMapRef,
        @Nullable final String stateRef
    )
    {
        snapshotVlmDfnUuid = snapshotVlmDfnUuidRef;
        snapshotVlmUuid = snapshotVlmUuidRef;
        snapshotVlmNr = snapshotVlmNrRef;
        state = stateRef;
        snapVlmPropsMap = Collections.unmodifiableMap(snapVlmPropsMapRef);
        vlmPropsMap = Collections.unmodifiableMap(vlmPropsMapRef);
    }

    @Override
    public UUID getSnapshotVlmUuid()
    {
        return snapshotVlmUuid;
    }

    @Override
    public UUID getSnapshotVlmDfnUuid()
    {
        return snapshotVlmDfnUuid;
    }

    @Override
    public int getSnapshotVlmNr()
    {
        return snapshotVlmNr;
    }

    @Override
    public Map<String, String> getSnapVlmPropsMap()
    {
        return snapVlmPropsMap;
    }

    @Override
    public Map<String, String> getVlmPropsMap()
    {
        return vlmPropsMap;
    }

    @Override
    public @Nullable String getState()
    {
        return state;
    }
}
