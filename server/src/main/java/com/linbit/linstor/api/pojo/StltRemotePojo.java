package com.linbit.linstor.api.pojo;

import java.util.Map;
import java.util.UUID;

public class StltRemotePojo implements Comparable<StltRemotePojo>
{
    private final UUID uuid;
    private final String remoteName;
    private final String linRemoteName;
    private final long flags;
    private final String ip;
    private final Map<String, Integer> ports;
    private final Boolean useZstd;
    private final Long fullSyncId;
    private final Long updateId;

    public StltRemotePojo(
        UUID uuidRef,
        String remoteNameRef,
        String linRemoteNameRef,
        long flagsRef,
        String ipRet,
        Map<String, Integer> portsRef,
        Boolean useZstdRef,
        Long fullSyncIdRef,
        Long updateIdRef
    )
    {
        uuid = uuidRef;
        remoteName = remoteNameRef;
        linRemoteName = linRemoteNameRef;
        flags = flagsRef;
        ip = ipRet;
        ports = portsRef;
        useZstd = useZstdRef;
        fullSyncId = fullSyncIdRef;
        updateId = updateIdRef;
    }

    public UUID getUuid()
    {
        return uuid;
    }

    public String getRemoteName()
    {
        return remoteName;
    }

    public String getLinRemoteName()
    {
        return linRemoteName;
    }

    public long getFlags()
    {
        return flags;
    }

    public String getIp()
    {
        return ip;
    }

    public Map<String, Integer> getPorts()
    {
        return ports;
    }

    public Boolean useZstd()
    {
        return useZstd;
    }

    public Long getFullSyncId()
    {
        return fullSyncId;
    }

    public Long getUpdateId()
    {
        return updateId;
    }

    @Override
    public int compareTo(StltRemotePojo otherS3Remote)
    {
        return remoteName.compareTo(otherS3Remote.remoteName);
    }
}
