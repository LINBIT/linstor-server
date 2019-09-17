package com.linbit.linstor.core.apis;

import com.linbit.linstor.netcom.Peer;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface NodeApi
{
    String getName();
    String getType();
    UUID getUuid();
    Peer.ConnectionStatus connectionStatus();
    Map<String, String> getProps();
    long getFlags();
    List<NetInterfaceApi> getNetInterfaces();
    NetInterfaceApi getActiveStltConn();
}