package com.linbit.linstor.api.interfaces;

import com.linbit.linstor.storage.kinds.DeviceProviderKind;

/**
 * Marker interface
 */
public interface VlmLayerDataPojo
{
    int getVlmNr();

    DeviceProviderKind getProviderKind();

    String getDevicePath();

    long getAllocatedSize();

    long getUsableSize();
}
