package com.linbit.linstor.core.repository;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.ResourceDefinition;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Holds the singleton resource definition map instance.
 */
@Singleton
public class ResourceDefinitionRepositoryImpl implements ResourceDefinitionRepository
{
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final CoreModule.ResourceDefinitionMapExtName rscDfnMapExtName;

    @Inject
    public ResourceDefinitionRepositoryImpl(CoreModule.ResourceDefinitionMap rscDfnMapRef,
                                                  CoreModule.ResourceDefinitionMapExtName rscDfnMapExtNameRef)
    {
        rscDfnMap = rscDfnMapRef;
        rscDfnMapExtName = rscDfnMapExtNameRef;
    }

    @Override
    public @Nullable ResourceDefinition get(ResourceName resourceName)
    {
        return rscDfnMap.get(resourceName);
    }

    @Override
    public ResourceDefinition get(byte[] externalName)
    {
        return rscDfnMapExtName.get(externalName);
    }

    @Override
    public void put(ResourceDefinition resourceDefinition)
    {

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
    public void remove(ResourceName resourceName, byte[] externalName)
    {
        rscDfnMap.remove(resourceName);
        if (externalName != null)
        {
            rscDfnMapExtName.remove(externalName);
        }
    }

    @Override
    public CoreModule.ResourceDefinitionMap getMapForView()
    {
        return rscDfnMap;
    }

    @Override
    public CoreModule.ResourceDefinitionMapExtName getMapForViewExtName()
    {
        return rscDfnMapExtName;
    }

}
