package com.linbit.linstor.api.pojo.backups;

public class ScheduleDetailsPojo
{
    private final String remote;
    private final String schedule;
    private final Boolean ctrl;
    private final Boolean rscGrp;
    private final Boolean rscDfn;

    public ScheduleDetailsPojo(
        String remoteRef,
        String scheduleRef,
        Boolean ctrlRef,
        Boolean rscGrpRef,
        Boolean rscDfnRef
    )
    {
        remote = remoteRef;
        schedule = scheduleRef;
        ctrl = ctrlRef;
        rscGrp = rscGrpRef;
        rscDfn = rscDfnRef;
    }

    public String getRemote()
    {
        return remote;
    }

    public String getSchedule()
    {
        return schedule;
    }

    public Boolean getCtrl()
    {
        return ctrl;
    }

    public Boolean getRscGrp()
    {
        return rscGrp;
    }

    public Boolean getRscDfn()
    {
        return rscDfn;
    }
}
