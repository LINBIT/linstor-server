package com.linbit.linstor.api.pojo;

import java.util.UUID;

public class StltRemotePojo implements Comparable<StltRemotePojo>
{
    private final UUID uuid;
    private final String remoteName;
    private final long flags;
    private final String ip;
    private final Integer port;
    private final Boolean useZstd;
    private final Long fullSyncId;
    private final Long updateId;

    public StltRemotePojo(
        UUID uuidRef,
        String remoteNameRef,
        long flagsRef,
        String ipRet,
        Integer portRef,
        Boolean useZstdRef,
        Long fullSyncIdRef,
        Long updateIdRef
    )
    {
        uuid = uuidRef;
        remoteName = remoteNameRef;
        flags = flagsRef;
        ip = ipRet;
        port = portRef;
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

    public long getFlags()
    {
        return flags;
    }

    public String getIp()
    {
        return ip;
    }

    public Integer getPort()
    {
        return port;
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
