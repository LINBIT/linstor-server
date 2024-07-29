package com.linbit.linstor.core.apis;

import com.linbit.linstor.annotation.Nullable;

import java.util.UUID;

public interface NetInterfaceApi
{
    UUID getUuid();
    String getName();
    String getAddress();

    boolean isUsableAsSatelliteConnection();

    @Nullable
    Integer getSatelliteConnectionPort();

    @Nullable
    String getSatelliteConnectionEncryptionType();
}
