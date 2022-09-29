package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.apis.SnapshotVolumeApi;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class SnapshotVlmPojo implements SnapshotVolumeApi
{
    private final UUID snapshotVlmDfnUuid;
    private final UUID snapshotVlmUuid;
    private final int snapshotVlmNr;
    private final Map<String, String> propsMap;
    private final String state;

    public SnapshotVlmPojo(
        final UUID snapshotVlmDfnUuidRef,
        final UUID snapshotVlmUuidRef,
        final int snapshotVlmNrRef,
        Map<String, String> propsMapRef,
        @Nullable final String stateRef
    )
    {
        snapshotVlmDfnUuid = snapshotVlmDfnUuidRef;
        snapshotVlmUuid = snapshotVlmUuidRef;
        snapshotVlmNr = snapshotVlmNrRef;
        state = stateRef;
        propsMap = Collections.unmodifiableMap(propsMapRef);
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
    public Map<String, String> getPropsMap()
    {
        return propsMap;
    }

    @Override
    public String getState()
    {
        return state;
    }
}
