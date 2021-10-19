package com.linbit.linstor.core.apis;

import java.util.UUID;

public interface NetInterfaceApi
{
    UUID getUuid();
    String getName();
    String getAddress();

    boolean isUsableAsSatelliteConnection();

    int getSatelliteConnectionPort();
    String getSatelliteConnectionEncryptionType();
}
