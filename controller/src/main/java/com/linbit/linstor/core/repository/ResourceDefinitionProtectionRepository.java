package com.linbit.linstor.core.repository;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.ResourceDefinition;
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
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final CoreModule.ResourceDefinitionMapExtName rscDfnMapExtName;
    private @Nullable ObjectProtection resourceDefinitionMapObjProt;

    @Inject
    public ResourceDefinitionProtectionRepository(CoreModule.ResourceDefinitionMap rscDfnMapRef,
                                                  CoreModule.ResourceDefinitionMapExtName rscDfnMapExtNameRef)
    {
        rscDfnMap = rscDfnMapRef;
        rscDfnMapExtName = rscDfnMapExtNameRef;
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
    public @Nullable ResourceDefinition get(AccessContext accCtx, ResourceName resourceName)
        throws AccessDeniedException
    {
        checkProtSet();
        resourceDefinitionMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return rscDfnMap.get(resourceName);
    }

    @Override
    public ResourceDefinition get(AccessContext accCtx, byte[] externalName)
        throws AccessDeniedException
    {
        checkProtSet();
        resourceDefinitionMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return rscDfnMapExtName.get(externalName);
    }

    @Override
    public void put(AccessContext accCtx, ResourceDefinition resourceDefinition)
        throws AccessDeniedException
    {
        checkProtSet();
        resourceDefinitionMapObjProt.requireAccess(accCtx, AccessType.CHANGE);

        ResourceDefinition rscDfn = rscDfnMap.put(resourceDefinition.getName(), resourceDefinition);
        if (rscDfn != null)
        {
            throw new ImplementationError("Resource definition name already exists!");
        }
        if (resourceDefinition.getExternalName() != null)
        {
            rscDfnMapExtName.put(resourceDefinition.getExternalName(), resourceDefinition);
        }
    }


    @Override
    public void remove(AccessContext accCtx, ResourceName resourceName, byte[] externalName)
        throws AccessDeniedException
    {
        checkProtSet();
        resourceDefinitionMapObjProt.requireAccess(accCtx, AccessType.CHANGE);
        rscDfnMap.remove(resourceName);
        if (externalName != null)
        {
            rscDfnMapExtName.remove(externalName);
        }
    }

    @Override
    public CoreModule.ResourceDefinitionMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkProtSet();
        resourceDefinitionMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return rscDfnMap;
    }

    @Override
    public CoreModule.ResourceDefinitionMapExtName getMapForViewExtName(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkProtSet();
        resourceDefinitionMapObjProt.requireAccess(accCtx, AccessType.VIEW);
        return rscDfnMapExtName;
    }

    private void checkProtSet()
    {
        if (resourceDefinitionMapObjProt == null)
        {
            throw new IllegalStateException("Object protection not yet set");
        }
    }
}
