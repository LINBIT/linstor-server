package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Holds the singleton KeyValueStore map protection instance, allowing it to be initialized from the database
 * after dependency injection has been performed.
 */
@Singleton
public class ResourceGroupProtectionRepository implements ResourceGroupRepository
{
    private final CoreModule.ResourceGroupMap rscGrpMap;
    private @Nullable ObjectProtection rscGrpMapObjProt;

    @Inject
    public ResourceGroupProtectionRepository(CoreModule.ResourceGroupMap rscGrpRef)
    {
        rscGrpMap = rscGrpRef;
    }

    public void setObjectProtection(ObjectProtection rscGrpObjProtRef)
    {
        if (rscGrpMapObjProt != null)
        {
            throw new IllegalStateException("Object protection already set");
        }
        rscGrpMapObjProt = rscGrpObjProtRef;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkProtSet();
        return rscGrpMapObjProt;
    }

    @Override
    public void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException
    {
        checkProtSet();
        rscGrpMapObjProt.requireAccess(accCtx, requested);
    }

    @Override
    public @Nullable ResourceGroup get(
        AccessContext accCtx,
        ResourceGroupName rscGrpName
    )
        throws AccessDeniedException
    {
        checkProtSet();
        rscGrpMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return rscGrpMap.get(rscGrpName);
    }

    @Override
    public void put(AccessContext accCtx, ResourceGroup rscGrp)
        throws AccessDeniedException
    {
        checkProtSet();
        rscGrpMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        rscGrpMap.put(rscGrp.getName(), rscGrp);
    }

    @Override
    public void remove(AccessContext accCtx, ResourceGroupName rscGrpName)
        throws AccessDeniedException
    {
        checkProtSet();
        rscGrpMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        rscGrpMap.remove(rscGrpName);
    }

    @Override
    public CoreModule.ResourceGroupMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkProtSet();
        rscGrpMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return rscGrpMap;
    }

    private void checkProtSet()
    {
        if (rscGrpMapObjProt == null)
        {
            throw new IllegalStateException("Object protection not yet set");
        }
    }
}
