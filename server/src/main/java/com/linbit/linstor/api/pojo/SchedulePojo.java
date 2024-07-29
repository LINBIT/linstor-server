package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;

import java.util.UUID;

public class SchedulePojo implements Comparable<SchedulePojo>
{
    private final UUID uuid;
    private final String scheduleName;
    private final long flags;
    private final String fullCron;
    private final @Nullable String incCron;
    private final Integer keepLocal;
    private final Integer keepRemote;
    private final String onFailure;
    private final Integer maxRetries;
    private final @Nullable Long fullSyncId;
    private final @Nullable Long updateId;

    public SchedulePojo(
        UUID uuidRef,
        String scheduleNameRef,
        long flagsRef,
        String fullCronRef,
        @Nullable String incCronRef,
        Integer keepLocalRef,
        Integer keepRemoteRef,
        String onFailureRef,
        Integer maxRetriesRef,
        @Nullable Long fullSyncIdRef,
        @Nullable Long updateIdRef
    )
    {
        uuid = uuidRef;
        scheduleName = scheduleNameRef;
        flags = flagsRef;
        fullCron = fullCronRef;
        incCron = incCronRef;
        keepLocal = keepLocalRef;
        keepRemote = keepRemoteRef;
        onFailure = onFailureRef;
        maxRetries = maxRetriesRef;
        fullSyncId = fullSyncIdRef;
        updateId = updateIdRef;
    }

    public UUID getUuid()
    {
        return uuid;
    }

    public String getScheduleName()
    {
        return scheduleName;
    }

    public long getFlags()
    {
        return flags;
    }

    public String getFullCron()
    {
        return fullCron;
    }

    public @Nullable String getIncCron()
    {
        return incCron;
    }

    public Integer getKeepLocal()
    {
        return keepLocal;
    }

    public Integer getKeepRemote()
    {
        return keepRemote;
    }

    public String getOnFailure()
    {
        return onFailure;
    }

    public Integer getMaxRetries()
    {
        return maxRetries;
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
    public int compareTo(SchedulePojo other)
    {
        return scheduleName.compareTo(other.scheduleName);
    }
}
