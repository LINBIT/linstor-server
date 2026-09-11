package com.linbit.linstor.core.objects.remotes;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.remotes.S3RemoteDatabaseDriver;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.UUID;

@Singleton
public class S3RemoteControllerFactory
{
    private final S3RemoteDatabaseDriver dbDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final RemoteRepository remoteRepo;

    @Inject
    public S3RemoteControllerFactory(
        S3RemoteDatabaseDriver dbDriverRef,
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

    public S3Remote create(
        RemoteName nameRef,
        String endpointRef,
        String bucketRef,
        String regionRef,
        byte[] accessKeyRef,
        byte[] secretKeyRef
    )
        throws LinStorDataAlreadyExistsException, DatabaseException
    {
        if (remoteRepo.get(nameRef) != null)
        {
            throw new LinStorDataAlreadyExistsException("This remote name is already registered");
        }

        S3Remote remote = new S3Remote(
            UUID.randomUUID(),
            dbDriver,
            nameRef,
            0,
            endpointRef,
            bucketRef,
            regionRef,
            accessKeyRef,
            secretKeyRef,
            transObjFactory,
            transMgrProvider
        );

        dbDriver.create(remote);

        return remote;
    }
}
