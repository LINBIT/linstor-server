package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;

import java.util.Objects;
import java.util.UUID;

public class EbsRemotePojo implements Comparable<EbsRemotePojo>
{
    private final UUID uuid;
    private final String remoteName;
    private final long flags;
    private final String url;
    private final String availabilityZone;
    private final String region;
    private final byte[] accessKey;
    private final byte[] secretKey;
    private final @Nullable Long fullSyncId;
    private final @Nullable Long updateId;

    public EbsRemotePojo(
        UUID uuidRef,
        String remoteNameRef,
        long flagsRef,
        String urlRef,
        String availabilityZoneRef,
        String regionRef,
        byte[] accessKeyRef,
        byte[] secretKeyRef,
        @Nullable Long fullSyncIdRef,
        @Nullable Long updateIdRef
    )
    {
        uuid = uuidRef;
        remoteName = remoteNameRef;
        flags = flagsRef;
        url = urlRef;
        availabilityZone = availabilityZoneRef;
        region = regionRef;
        accessKey = accessKeyRef;
        secretKey = secretKeyRef;
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

    public String getUrl()
    {
        return url;
    }

    public String getAvailabilityZone()
    {
        return availabilityZone;
    }

    public String getRegion()
    {
        return region;
    }

    public byte[] getAccessKey()
    {
        return accessKey;
    }

    public byte[] getSecretKey()
    {
        return secretKey;
    }

    public @Nullable Long getFullSyncId()
    {
        return fullSyncId;
    }

    public @Nullable Long getUpdateId()
    {
        return updateId;
    }

    @Override
    public int compareTo(EbsRemotePojo otherS3Remote)
    {
        return remoteName.compareTo(otherS3Remote.remoteName);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((remoteName == null) ? 0 : remoteName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean eq = false;
        if (obj != null && getClass() == obj.getClass())
        {
            EbsRemotePojo other = (EbsRemotePojo) obj;
            eq = Objects.equals(other.remoteName, remoteName);
        }
        return eq;
    }
}
