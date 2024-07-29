package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;

import java.util.UUID;

public class S3RemotePojo implements Comparable<S3RemotePojo>
{
    private final UUID uuid;
    private final String remoteName;
    private final long flags;
    private final String endpoint;
    private final String bucket;
    private final String region;
    private final byte[] accessKey;
    private final byte[] secretKey;
    private final @Nullable Long fullSyncId;
    private final @Nullable Long updateId;

    public S3RemotePojo(
        UUID uuidRef,
        String remoteNameRef,
        long flagsRef,
        String endpointRef,
        String bucketRef,
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
        endpoint = endpointRef;
        bucket = bucketRef;
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

    public String getEndpoint()
    {
        return endpoint;
    }

    public String getBucket()
    {
        return bucket;
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
    public int compareTo(S3RemotePojo otherS3Remote)
    {
        return remoteName.compareTo(otherS3Remote.remoteName);
    }
}
