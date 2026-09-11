package com.linbit.linstor.core.objects.remotes;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.remotes.LinstorRemoteDatabaseDriver;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import com.linbit.linstor.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.net.URL;
import java.util.UUID;

@Singleton
public class LinstorRemoteControllerFactory
{
    private final LinstorRemoteDatabaseDriver dbDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final RemoteRepository remoteRepo;

    @Inject
    public LinstorRemoteControllerFactory(
        LinstorRemoteDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        RemoteRepository extFileRepoRef
    )
    {
        dbDriver = dbDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        remoteRepo = extFileRepoRef;
    }

    public LinstorRemote create(
        RemoteName nameRef,
        URL url,
        @Nullable byte[] encryptedPassphraseRef,
        @Nullable UUID remoteClusterId
    )
        throws LinStorDataAlreadyExistsException, DatabaseException
    {
        if (remoteRepo.get(nameRef) != null)
        {
            throw new LinStorDataAlreadyExistsException("This remote name is already registered");
        }

        LinstorRemote remote = new LinstorRemote(
            UUID.randomUUID(),
            dbDriver,
            nameRef,
            0,
            url,
            encryptedPassphraseRef,
            remoteClusterId,
            transObjFactory,
            transMgrProvider
        );

        dbDriver.create(remote);

        return remote;
    }
}
