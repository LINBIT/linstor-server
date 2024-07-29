package com.linbit.linstor.api.pojo.backups;

import com.linbit.linstor.annotation.Nullable;

public class ScheduledRscsPojo
{
    public final String rsc_name;
    public final @Nullable String remote;
    public final @Nullable String schedule;
    public final @Nullable String reason;
    public final long last_snap_time;
    public final boolean last_snap_inc;
    public final long next_exec_time;
    public final boolean next_exec_inc;
    public final long next_planned_full;
    public final long next_planned_inc;

    public ScheduledRscsPojo(
        String rsc_nameRef,
        @Nullable String remoteRef,
        @Nullable String scheduleRef,
        @Nullable String reasonRef,
        long lastSnapTimeRef,
        boolean lastSnapIncRef,
        long nextExecTimeRef,
        boolean nextExecIncRef,
        long nextPlannedFullRef,
        long nextPlannedIncRef
    )
    {
        rsc_name = rsc_nameRef;
        remote = remoteRef;
        schedule = scheduleRef;
        reason = reasonRef;
        last_snap_time = lastSnapTimeRef;
        last_snap_inc = lastSnapIncRef;
        next_exec_time = nextExecTimeRef;
        next_exec_inc = nextExecIncRef;
        next_planned_full = nextPlannedFullRef;
        next_planned_inc = nextPlannedIncRef;
    }

    public String getRsc_name()
    {
        return rsc_name;
    }

    public @Nullable String getRemote()
    {
        return remote;
    }

    public @Nullable String getSchedule()
    {
        return schedule;
    }

    public long getLastSnapTime()
    {
        return last_snap_time;
    }

    public boolean getLastSnapInc()
    {
        return last_snap_inc;
    }

    public long getNextExecTime()
    {
        return next_exec_time;
    }

    public boolean getNextExecInc()
    {
        return next_exec_inc;
    }

    public long getNextPlannedFull()
    {
        return next_planned_full;
    }

    public long getNextPlannedInc()
    {
        return next_planned_inc;
    }
}
