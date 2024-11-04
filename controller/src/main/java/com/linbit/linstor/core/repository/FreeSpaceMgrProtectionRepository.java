package com.linbit.linstor.core.repository;

import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Singleton;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Holds the singleton free space manager map protection instance, allowing it to be initialized from the database
 * after dependency injection has been performed.
 */
@Singleton
public class FreeSpaceMgrProtectionRepository implements FreeSpaceMgrRepository
{
    private final ControllerCoreModule.FreeSpaceMgrMap freeSpaceMgrMap;
    private ObjectProtection freeSpaceMgrMapObjProt;

    // can't initialize objProt in constructor because of chicken-egg-problem
    @SuppressFBWarnings("NP_NONNULL_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
    @Inject
    public FreeSpaceMgrProtectionRepository(ControllerCoreModule.FreeSpaceMgrMap freeSpaceMgrMapRef)
    {
        freeSpaceMgrMap = freeSpaceMgrMapRef;
    }

    public void setObjectProtection(ObjectProtection freeSpaceMgrMapObjProtRef)
    {
        if (freeSpaceMgrMapObjProt != null)
        {
            throw new IllegalStateException("Object protection already set");
        }
        freeSpaceMgrMapObjProt = freeSpaceMgrMapObjProtRef;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkProtSet();
        return freeSpaceMgrMapObjProt;
    }

    @Override
    public void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException
    {
        checkProtSet();
        freeSpaceMgrMapObjProt.requireAccess(accCtx, requested);
    }

    @Override
    public FreeSpaceMgr get(
        AccessContext accCtx,
        SharedStorPoolName sharedStorPoolName
    )
        throws AccessDeniedException
    {
        checkProtSet();
        freeSpaceMgrMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return freeSpaceMgrMap.get(sharedStorPoolName);
    }

    @Override
    public void put(AccessContext accCtx, SharedStorPoolName sharedStorPoolName, FreeSpaceMgr freeSpaceMgr)
        throws AccessDeniedException
    {
        checkProtSet();
        freeSpaceMgrMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        freeSpaceMgrMap.put(sharedStorPoolName, freeSpaceMgr);
    }

    @Override
    public void remove(AccessContext accCtx, SharedStorPoolName sharedStorPoolName)
        throws AccessDeniedException
    {
        checkProtSet();
        freeSpaceMgrMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        freeSpaceMgrMap.remove(sharedStorPoolName);
    }

    @Override
    public ControllerCoreModule.FreeSpaceMgrMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkProtSet();
        freeSpaceMgrMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return freeSpaceMgrMap;
    }

    private void checkProtSet()
    {
        if (freeSpaceMgrMapObjProt == null)
        {
            throw new IllegalStateException("Object protection not yet set");
        }
    }
}
