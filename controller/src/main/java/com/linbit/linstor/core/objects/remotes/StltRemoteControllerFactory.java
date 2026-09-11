package com.linbit.linstor.core.objects.remotes;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.noop.NoOpFlagDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.Map;
import java.util.UUID;

@Singleton
public class StltRemoteControllerFactory
{
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final RemoteRepository remoteRepo;
    private final StateFlagsPersistence<?> stateFlagsDriver = new NoOpFlagDriver();

    @Inject
    public StltRemoteControllerFactory(
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        RemoteRepository extFileRepoRef
    )
    {
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        remoteRepo = extFileRepoRef;
    }

    public StltRemote create(
        RemoteName nameRef,
        @Nullable String ipRef,
        Map<String, Integer> portsRef,
        RemoteName linstorRemoteNameRef,
        @Nullable Node nodeRef,
        String otherRscNameRef
    )
        throws DatabaseException
    {
        if (remoteRepo.get(nameRef) != null)
        {
            throw new ImplementationError("This remote name is already registered");
        }

        return new StltRemote(
            UUID.randomUUID(),
            nameRef,
            0,
            ipRef,
            portsRef,
            null,
            linstorRemoteNameRef,
            nodeRef,
            (StateFlagsPersistence<StltRemote>) stateFlagsDriver,
            transObjFactory,
            transMgrProvider,
            otherRscNameRef
        );
    }
}
