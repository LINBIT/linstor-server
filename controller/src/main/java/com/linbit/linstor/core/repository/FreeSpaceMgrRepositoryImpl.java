package com.linbit.linstor.core.repository;

import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.FreeSpaceMgr;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Holds the singleton free space manager map instance.
 */
@Singleton
public class FreeSpaceMgrRepositoryImpl implements FreeSpaceMgrRepository
{
    private final ControllerCoreModule.FreeSpaceMgrMap freeSpaceMgrMap;

    @Inject
    public FreeSpaceMgrRepositoryImpl(ControllerCoreModule.FreeSpaceMgrMap freeSpaceMgrMapRef)
    {
        freeSpaceMgrMap = freeSpaceMgrMapRef;
    }

    @Override
    public FreeSpaceMgr get(
        SharedStorPoolName sharedStorPoolName
    )
    {
        return freeSpaceMgrMap.get(sharedStorPoolName);
    }

    @Override
    public void put(SharedStorPoolName sharedStorPoolName, FreeSpaceMgr freeSpaceMgr)
    {
        freeSpaceMgrMap.put(sharedStorPoolName, freeSpaceMgr);
    }

    @Override
    public void remove(SharedStorPoolName sharedStorPoolName)
    {
        freeSpaceMgrMap.remove(sharedStorPoolName);
    }

    @Override
    public ControllerCoreModule.FreeSpaceMgrMap getMapForView()
    {
        return freeSpaceMgrMap;
    }

}
