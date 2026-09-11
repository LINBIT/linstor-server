package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.AbsRemote;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class RemoteRepositoryImpl implements RemoteRepository
{
    private final CoreModule.RemoteMap remoteMap;

    @Inject
    public RemoteRepositoryImpl(CoreModule.RemoteMap remoteMapRef)
    {
        remoteMap = remoteMapRef;
    }

    @Override
    public @Nullable AbsRemote get(RemoteName remoteNameRef)
    {
        return remoteMap.get(remoteNameRef);
    }

    @Override
    public void put(AbsRemote remoteRef)
    {
        remoteMap.put(remoteRef.getName(), remoteRef);
    }

    @Override
    public void remove(RemoteName remoteNameRef)
    {
        remoteMap.remove(remoteNameRef);
    }

    @Override
    public RemoteMap getMapForView()
    {
        return remoteMap;
    }

}
