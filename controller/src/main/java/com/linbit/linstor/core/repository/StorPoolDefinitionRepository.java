package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ProtectedObject;

/**
 * Provides access to stor pool definitions with automatic security checks.
 */
public interface StorPoolDefinitionRepository extends ProtectedObject
{
    void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException;

    @Nullable
    StorPoolDefinition get(AccessContext accCtx, StorPoolName nameRef)
        throws AccessDeniedException;

    void put(AccessContext accCtx, StorPoolName storPoolName, StorPoolDefinition storPoolDefinition)
        throws AccessDeniedException;

    void remove(AccessContext accCtx, StorPoolName storPoolName)
        throws AccessDeniedException;

    CoreModule.StorPoolDefinitionMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException;
}
