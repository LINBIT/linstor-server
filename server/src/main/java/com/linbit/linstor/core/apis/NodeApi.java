package com.linbit.linstor.core.apis;

import com.linbit.linstor.api.ApiConsts;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface NodeApi
{
    String getName();
    String getType();
    UUID getUuid();
    ApiConsts.ConnectionStatus connectionStatus();
    long getReconnectAttemptCount();
    Map<String, String> getProps();
    long getFlags();
    List<NetInterfaceApi> getNetInterfaces();
    NetInterfaceApi getActiveStltConn();
    List<String> getDeviceLayerKindNames();
    List<String> getDeviceProviderKindNames();
    Map<String, List<String>> getUnsupportedLayersWithReasons();
    Map<String, List<String>> getUnsupportedProvidersWithReasons();
    Long getEvictionTimestamp();
}
