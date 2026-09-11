package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ExternalFileMap;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.objects.ExternalFile;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class ExternalFileRepositoryImpl implements ExternalFileRepository
{
    private final CoreModule.ExternalFileMap extFileMap;

    @Inject
    public ExternalFileRepositoryImpl(CoreModule.ExternalFileMap extFileMapRef)
    {
        extFileMap = extFileMapRef;
    }

    @Override
    public @Nullable ExternalFile get(ExternalFileName externalFileNameRef)
    {
        return extFileMap.get(externalFileNameRef);
    }

    @Override
    public void put(ExternalFile externalFileRef)
    {
        extFileMap.put(externalFileRef.getName(), externalFileRef);
    }

    @Override
    public void remove(ExternalFileName externalFileNameRef)
    {
        extFileMap.remove(externalFileNameRef);
    }

    @Override
    public ExternalFileMap getMapForView()
    {
        return extFileMap;
    }

}
