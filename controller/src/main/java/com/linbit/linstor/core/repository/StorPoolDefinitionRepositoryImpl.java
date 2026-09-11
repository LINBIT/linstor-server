package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.StorPoolDefinition;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Holds the singleton stor pool definition map instance.
 */
@Singleton
public class StorPoolDefinitionRepositoryImpl implements StorPoolDefinitionRepository
{
    private final CoreModule.StorPoolDefinitionMap storPoolDefinitionMap;

    @Inject
    public StorPoolDefinitionRepositoryImpl(CoreModule.StorPoolDefinitionMap storPoolDefinitionMapRef)
    {
        storPoolDefinitionMap = storPoolDefinitionMapRef;
    }

    @Override
    public @Nullable StorPoolDefinition get(
        StorPoolName storPoolName
    )
    {
        return storPoolDefinitionMap.get(storPoolName);
    }

    @Override
    public void put(StorPoolName storPoolName, StorPoolDefinition storPoolDefinition)
    {
        storPoolDefinitionMap.put(storPoolName, storPoolDefinition);
    }

    @Override
    public void remove(StorPoolName storPoolName)
    {
        storPoolDefinitionMap.remove(storPoolName);
    }

    @Override
    public CoreModule.StorPoolDefinitionMap getMapForView()
    {
        return storPoolDefinitionMap;
    }

}
