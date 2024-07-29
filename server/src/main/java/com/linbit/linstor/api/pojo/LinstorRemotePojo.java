package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;

import java.util.UUID;

public class LinstorRemotePojo implements Comparable<LinstorRemotePojo>
{
    private final UUID uuid;
    private final String remoteName;
    private final long flags;
    private final String url;
    // intentionally NO encryptedPassphrase. will not be serialized

    private final @Nullable Long fullSyncId;
    private final @Nullable Long updateId;

    public LinstorRemotePojo(
        UUID uuidRef,
        String remoteNameRef,
        long flagsRef,
        String urlStrRef,
        @Nullable Long fullSyncIdRef,
        @Nullable Long updateIdRef
    )
    {
        uuid = uuidRef;
        remoteName = remoteNameRef;
        flags = flagsRef;
        url = urlStrRef;
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

    public @Nullable Long getFullSyncId()
    {
        return fullSyncId;
    }

    public @Nullable Long getUpdateId()
    {
        return updateId;
    }

    @Override
    public int compareTo(LinstorRemotePojo otherLinstorRemote)
    {
        return remoteName.compareTo(otherLinstorRemote.remoteName);
    }
}
