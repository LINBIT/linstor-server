package com.linbit.linstor.core.objects.remotes;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.remotes.LinstorRemoteDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import com.linbit.linstor.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.net.URL;
import java.util.UUID;

@Singleton
public class LinstorRemoteControllerFactory
{
    private final LinstorRemoteDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objProtFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final RemoteRepository remoteRepo;

    @Inject
    public LinstorRemoteControllerFactory(
        LinstorRemoteDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objProtFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        RemoteRepository extFileRepoRef
    )
    {
        dbDriver = dbDriverRef;
        objProtFactory = objProtFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        remoteRepo = extFileRepoRef;
    }

    public LinstorRemote create(
        AccessContext accCtxRef,
        RemoteName nameRef,
        URL url,
        @Nullable byte[] encryptedPassphraseRef,
        @Nullable UUID remoteClusterId
    )
        throws AccessDeniedException, LinStorDataAlreadyExistsException, DatabaseException
    {
        if (remoteRepo.get(accCtxRef, nameRef) != null)
        {
            throw new LinStorDataAlreadyExistsException("This remote name is already registered");
        }

        LinstorRemote remote = new LinstorRemote(
            objProtFactory.getInstance(
                accCtxRef,
                ObjectProtection.buildPath(nameRef),
                true
            ),
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
