package com.linbit.linstor;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

/**
 * Provides access to stor pool definitions with automatic security checks.
 */
public interface FreeSpaceMgrRepository
{
    ObjectProtection getObjProt();

    void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException;

    FreeSpaceMgr get(AccessContext accCtx, FreeSpaceMgrName freeSpaceMgrName)
        throws AccessDeniedException;

    void put(AccessContext accCtx, FreeSpaceMgrName freeSpaceMgrName, FreeSpaceMgr freeSpaceMgr)
        throws AccessDeniedException;

    void remove(AccessContext accCtx, FreeSpaceMgrName freeSpaceMgrName)
        throws AccessDeniedException;

    CoreModule.FreeSpaceMgrMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException;
}
