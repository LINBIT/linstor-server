package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.objects.ResourceGroup;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Holds the singleton KeyValueStore map instance.
 */
@Singleton
public class ResourceGroupRepositoryImpl implements ResourceGroupRepository
{
    private final CoreModule.ResourceGroupMap rscGrpMap;

    @Inject
    public ResourceGroupRepositoryImpl(CoreModule.ResourceGroupMap rscGrpRef)
    {
        rscGrpMap = rscGrpRef;
    }

    @Override
    public @Nullable ResourceGroup get(
        ResourceGroupName rscGrpName
    )
    {
        return rscGrpMap.get(rscGrpName);
    }

    @Override
    public void put(ResourceGroup rscGrp)
    {
        rscGrpMap.put(rscGrp.getName(), rscGrp);
    }

    @Override
    public void remove(ResourceGroupName rscGrpName)
    {
        rscGrpMap.remove(rscGrpName);
    }

    @Override
    public CoreModule.ResourceGroupMap getMapForView()
    {
        return rscGrpMap;
    }

}
