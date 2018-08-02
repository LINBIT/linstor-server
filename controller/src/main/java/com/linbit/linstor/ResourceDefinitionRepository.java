package com.linbit.linstor;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

/**
 * Provides access to resource definitions with automatic security checks.
 */
public interface ResourceDefinitionRepository
{
    ObjectProtection getObjProt();

    void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException;

    ResourceDefinitionData get(AccessContext accCtx, ResourceName nameRef)
        throws AccessDeniedException;

    void put(AccessContext accCtx, ResourceName resourceName, ResourceDefinition resourceDefinition)
        throws AccessDeniedException;

    void remove(AccessContext accCtx, ResourceName resourceName)
        throws AccessDeniedException;

    CoreModule.ResourceDefinitionMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException;
}
