package com.linbit.linstor.core.apis;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.PairNonNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface VolumeApi
{
    UUID getVlmUuid();
    UUID getVlmDfnUuid();
    String getDevicePath();
    int getVlmNr();
    long getFlags();
    Map<String, String> getVlmProps();
    Optional<Long> getAllocatedSize();
    Optional<Long> getUsableSize();

    List<PairNonNull<String, VlmLayerDataApi>> getVlmLayerData();

    // the following methods should be removed, but will stay for a while for client-compatibility
    @Deprecated
    /** returns the name of the storage pool of the vlmLayerObject with "" as resource name suffix */
    String getStorPoolName();
    @Deprecated
    /** returns the DeviceProviderKind of the storage pool of the vlmLayerObject with "" as resource name suffix */
    DeviceProviderKind getStorPoolDeviceProviderKind();

    default Optional<StorPoolApi> getStorageStorPool()
    {
        return getVlmLayerData().stream()
            .filter(entry -> entry.objA.equalsIgnoreCase(DeviceLayerKind.STORAGE.name())).findFirst()
            .map(stringVlmLayerDataApiPair -> stringVlmLayerDataApiPair.objB.getStorPoolApi());
    }

    ApiCallRc getReports();
}
