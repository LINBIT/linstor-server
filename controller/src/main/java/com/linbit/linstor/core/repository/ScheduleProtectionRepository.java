package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ScheduleMap;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.objects.Schedule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ScheduleProtectionRepository implements ScheduleRepository
{
    private final CoreModule.ScheduleMap scheduleMap;
    private @Nullable ObjectProtection scheduleMapObjProt;

    @Inject
    public ScheduleProtectionRepository(CoreModule.ScheduleMap scheduleMapRef)
    {
        scheduleMap = scheduleMapRef;
    }

    public void setObjectProtection(ObjectProtection scheduleMapObjProtRef)
    {
        if (scheduleMapObjProt != null)
        {
            throw new IllegalStateException("Object protection already set");
        }
        scheduleMapObjProt = scheduleMapObjProtRef;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkProtSet();
        return scheduleMapObjProt;
    }

    @Override
    public void requireAccess(AccessContext accCtx, AccessType requested) throws AccessDeniedException
    {
        checkProtSet();
        scheduleMapObjProt.requireAccess(accCtx, requested);
    }

    @Override
    public @Nullable Schedule get(AccessContext accCtx, ScheduleName scheduleNameRef) throws AccessDeniedException
    {
        checkProtSet();
        requireAccess(accCtx, AccessType.VIEW);
        return scheduleMap.get(scheduleNameRef);
    }

    @Override
    public void put(AccessContext accCtx, Schedule scheduleRef) throws AccessDeniedException
    {
        checkProtSet();
        requireAccess(accCtx, AccessType.CHANGE);
        scheduleMap.put(scheduleRef.getName(), scheduleRef);
    }

    @Override
    public void remove(AccessContext accCtx, ScheduleName scheduleNameRef) throws AccessDeniedException
    {
        checkProtSet();
        requireAccess(accCtx, AccessType.CHANGE);
        scheduleMap.remove(scheduleNameRef);
    }

    @Override
    public ScheduleMap getMapForView(AccessContext accCtx) throws AccessDeniedException
    {
        checkProtSet();
        requireAccess(accCtx, AccessType.VIEW);
        return scheduleMap;
    }

    private void checkProtSet()
    {
        if (scheduleMapObjProt == null)
        {
            throw new IllegalStateException("Object protection not yet set");
        }
    }
}
