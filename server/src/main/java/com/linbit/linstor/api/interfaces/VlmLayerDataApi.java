package com.linbit.linstor.api.interfaces;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

/**
 * Marker interface
 */
public interface VlmLayerDataApi
{
    int getVlmNr();

    DeviceLayerKind getLayerKind();

    DeviceProviderKind getProviderKind();

    String getDevicePath();

    long getAllocatedSize();

    long getUsableSize();

    String getDiskState();
}
