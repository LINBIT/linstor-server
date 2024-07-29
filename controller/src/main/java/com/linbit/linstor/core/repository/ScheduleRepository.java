package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.objects.Schedule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ProtectedObject;

public interface ScheduleRepository extends ProtectedObject
{
    void requireAccess(AccessContext accCtx, AccessType requested) throws AccessDeniedException;

    @Nullable
    Schedule get(AccessContext accCtx, ScheduleName scheduleName) throws AccessDeniedException;

    void put(AccessContext accCtx, Schedule schedule) throws AccessDeniedException;

    void remove(AccessContext accCtx, ScheduleName scheduleName) throws AccessDeniedException;

    CoreModule.ScheduleMap getMapForView(AccessContext accCtx) throws AccessDeniedException;
}
