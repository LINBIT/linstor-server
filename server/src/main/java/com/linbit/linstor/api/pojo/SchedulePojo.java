package com.linbit.linstor.api.pojo;

import java.util.UUID;

public class SchedulePojo implements Comparable<SchedulePojo>
{
    private final UUID uuid;
    private final String scheduleName;
    private final long flags;
    private final String fullCron;
    private final String incCron;
    private final Integer keepLocal;
    private final Integer keepRemote;
    private final String onFailure;
    private final Integer maxRetries;
    private final Long fullSyncId;
    private final Long updateId;

    public SchedulePojo(
        UUID uuidRef,
        String scheduleNameRef,
        long flagsRef,
        String fullCronRef,
        String incCronRef,
        Integer keepLocalRef,
        Integer keepRemoteRef,
        String onFailureRef,
        Integer maxRetriesRef,
        Long fullSyncIdRef,
        Long updateIdRef
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

    public String getIncCron()
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

    public Long getFullSyncId()
    {
        return fullSyncId;
    }

    public Long getUpdateId()
    {
        return updateId;
    }

    @Override
    public int compareTo(SchedulePojo other)
    {
        return scheduleName.compareTo(other.scheduleName);
    }
}
