package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.apis.VolumeGroupApi;

import com.linbit.linstor.annotation.Nullable;

import java.util.Map;
import java.util.UUID;

public class VlmGrpPojo implements VolumeGroupApi
{
    private final UUID uuid;
    private final @Nullable Integer vlmNrInt;
    private final Map<String, String> propsMap;
    private final long flags;

    public VlmGrpPojo(
        UUID uuidRef,
        @Nullable Integer vlmNrIntRef,
        Map<String, String> propsMapRef,
        long flagsRef
    )
    {
        uuid = uuidRef;
        vlmNrInt = vlmNrIntRef;
        propsMap = propsMapRef;
        flags = flagsRef;
    }

    @Override
    public UUID getUUID()
    {
        return uuid;
    }

    @Override
    public @Nullable Integer getVolumeNr()
    {
        return vlmNrInt;
    }

    @Override
    public Map<String, String> getProps()
    {
        return propsMap;
    }

    @Override
    public long getFlags()
    {
        return flags;
    }
}
