package com.linbit.linstor.core.apis;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface StorPoolApi
{
    UUID getStorPoolUuid();
    String getStorPoolName();
    UUID getStorPoolDfnUuid();
    String getNodeName();
    UUID getNodeUuid();
    DeviceProviderKind getDeviceProviderKind();
    String getFreeSpaceManagerName();
    Optional<Long> getFreeCapacity();
    Optional<Long> getTotalCapacity();

    Map<String, String> getStorPoolProps();
    Map<String, String> getStorPoolStaticTraits();
    Map<String, String> getStorPoolDfnProps();
    ApiCallRc getReports();
    Boolean supportsSnapshots();
    Boolean isPmem();
}