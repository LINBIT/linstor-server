package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

/**
 * Provides access to {@link KeyValueStore}s with automatic security checks.
 */
public interface ResourceGroupRepository
{
    ObjectProtection getObjProt();

    void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException;

    @Nullable
    ResourceGroup get(AccessContext accCtx, ResourceGroupName nameRef)
        throws AccessDeniedException;

    void put(AccessContext accCtx, ResourceGroup rscGrpData)
        throws AccessDeniedException;

    void remove(AccessContext accCtx, ResourceGroupName rscGrpName)
        throws AccessDeniedException;

    CoreModule.ResourceGroupMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException;
}
