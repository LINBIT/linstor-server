package com.linbit.linstor;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Holds the singleton resource definition map protection instance, allowing it to be initialized from the database
 * after dependency injection has been performed.
 */
@Singleton
public class ResourceDefinitionProtectionRepository implements ResourceDefinitionRepository
{
    private final CoreModule.ResourceDefinitionMap resourceDefinitionMap;
    private ObjectProtection resourceDefinitionMapObjProt;

    @Inject
    public ResourceDefinitionProtectionRepository(CoreModule.ResourceDefinitionMap resourceDefinitionMapRef)
    {
        resourceDefinitionMap = resourceDefinitionMapRef;
    }

    public void setObjectProtection(ObjectProtection resourceDefinitionMapObjProtRef)
    {
        if (resourceDefinitionMapObjProt != null)
        {
            throw new IllegalStateException("Object protection already set");
        }
        resourceDefinitionMapObjProt = resourceDefinitionMapObjProtRef;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkProtSet();
        return resourceDefinitionMapObjProt;
    }

    @Override
    public void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException
    {
        checkProtSet();
        resourceDefinitionMapObjProt.requireAccess(accCtx, requested);
    }

    @Override
    public ResourceDefinitionData get(
        AccessContext accCtx,
        ResourceName resourceName
    )
        throws AccessDeniedException
    {
        checkProtSet();
        resourceDefinitionMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return (ResourceDefinitionData) resourceDefinitionMap.get(resourceName);
    }

    @Override
    public void put(AccessContext accCtx, ResourceName resourceName, ResourceDefinition resourceDefinition)
        throws AccessDeniedException
    {
        checkProtSet();
        resourceDefinitionMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        resourceDefinitionMap.put(resourceName, resourceDefinition);
    }

    @Override
    public void remove(AccessContext accCtx, ResourceName resourceName)
        throws AccessDeniedException
    {
        checkProtSet();
        resourceDefinitionMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        resourceDefinitionMap.remove(resourceName);
    }

    @Override
    public CoreModule.ResourceDefinitionMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkProtSet();
        resourceDefinitionMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceDefinitionMap;
    }

    private void checkProtSet()
    {
        if (resourceDefinitionMapObjProt == null)
        {
            throw new IllegalStateException("Object protection not yet set");
        }
    }
}
