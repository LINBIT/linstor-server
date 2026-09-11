package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ScheduleMap;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.objects.Schedule;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class ScheduleRepositoryImpl implements ScheduleRepository
{
    private final CoreModule.ScheduleMap scheduleMap;

    @Inject
    public ScheduleRepositoryImpl(CoreModule.ScheduleMap scheduleMapRef)
    {
        scheduleMap = scheduleMapRef;
    }

    @Override
    public @Nullable Schedule get(ScheduleName scheduleNameRef)
    {
        return scheduleMap.get(scheduleNameRef);
    }

    @Override
    public void put(Schedule scheduleRef)
    {
        scheduleMap.put(scheduleRef.getName(), scheduleRef);
    }

    @Override
    public void remove(ScheduleName scheduleNameRef)
    {
        scheduleMap.remove(scheduleNameRef);
    }

    @Override
    public ScheduleMap getMapForView()
    {
        return scheduleMap;
    }

}
