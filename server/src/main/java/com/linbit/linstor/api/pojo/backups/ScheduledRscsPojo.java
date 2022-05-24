package com.linbit.linstor.api.pojo.backups;

public class ScheduledRscsPojo
{
    public final String rsc_name;
    public final String remote;
    public final String schedule;
    public final String reason;
    public final long last_snap_time;
    public final boolean last_snap_inc;
    public final long next_exec_time;
    public final boolean next_exec_inc;
    public final long next_planned_full;
    public final long next_planned_inc;

    public ScheduledRscsPojo(
        String rsc_nameRef,
        String remoteRef,
        String scheduleRef,
        String reasonRef,
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

    public String getRemote()
    {
        return remote;
    }

    public String getSchedule()
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
