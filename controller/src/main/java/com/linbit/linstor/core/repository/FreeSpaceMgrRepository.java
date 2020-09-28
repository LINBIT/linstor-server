package com.linbit.linstor.core.repository;

import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

/**
 * Provides access to free space managers with automatic security checks.
 */
public interface FreeSpaceMgrRepository
{
    ObjectProtection getObjProt();

    void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException;

    FreeSpaceMgr get(AccessContext accCtx, SharedStorPoolName sharedStorPoolName)
        throws AccessDeniedException;

    void put(AccessContext accCtx, SharedStorPoolName sharedStorPoolName, FreeSpaceMgr freeSpaceMgr)
        throws AccessDeniedException;

    void remove(AccessContext accCtx, SharedStorPoolName sharedStorPoolName)
        throws AccessDeniedException;

    ControllerCoreModule.FreeSpaceMgrMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException;
}
