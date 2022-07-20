package com.linbit.linstor.core.objects.remotes;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.remotes.EbsRemoteDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.net.URL;
import java.util.UUID;

@Singleton
public class EbsRemoteControllerFactory
{
    private final EbsRemoteDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objProtFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final RemoteRepository remoteRepo;

    @Inject
    public EbsRemoteControllerFactory(
        EbsRemoteDatabaseDriver dbDriverRef,
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

    public EbsRemote create(
        AccessContext accCtxRef,
        RemoteName nameRef,
        long initFlagsRef,
        URL url,
        String regionRef,
        String availabilityZoneRef,
        byte[] encryptedAccessKeyRef,
        byte[] encryptedSecretKeyRef
    )
        throws AccessDeniedException, LinStorDataAlreadyExistsException, DatabaseException
    {
        if (remoteRepo.get(accCtxRef, nameRef) != null)
        {
            throw new LinStorDataAlreadyExistsException("This remote name is already registered");
        }

        EbsRemote remote = new EbsRemote(
            objProtFactory.getInstance(
                accCtxRef,
                ObjectProtection.buildPath(nameRef),
                true
            ),
            UUID.randomUUID(),
            dbDriver,
            nameRef,
            initFlagsRef,
            url,
            regionRef,
            availabilityZoneRef,
            encryptedSecretKeyRef,
            encryptedAccessKeyRef,
            transObjFactory,
            transMgrProvider
        );

        dbDriver.create(remote);

        return remote;
    }
}
