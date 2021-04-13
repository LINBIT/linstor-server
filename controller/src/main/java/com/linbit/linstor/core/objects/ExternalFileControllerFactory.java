package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.repository.ExternalFileRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ExternalFileDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.ByteUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.UUID;

@Singleton
public class ExternalFileControllerFactory
{
    private final ExternalFileDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objProtFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ExternalFileRepository extFileRepo;

    @Inject
    public ExternalFileControllerFactory(
        ExternalFileDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objProtFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ExternalFileRepository extFileRepoRef
    )
    {
        dbDriver = dbDriverRef;
        objProtFactory = objProtFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        extFileRepo = extFileRepoRef;
    }

    public ExternalFile create(
        AccessContext accCtxRef,
        ExternalFileName nameRef,
        byte[] contentRef
    )
        throws AccessDeniedException, LinStorDataAlreadyExistsException, DatabaseException
    {
        ExternalFile extFile = extFileRepo.get(accCtxRef, nameRef);
        if (extFile != null)
        {
            throw new LinStorDataAlreadyExistsException("This external file name is already registered");
        }

        extFile = new ExternalFile(
            UUID.randomUUID(),
            objProtFactory.getInstance(
                accCtxRef,
                ObjectProtection.buildPath(nameRef),
                true
            ),
            nameRef,
            0,
            contentRef,
            ByteUtils.checksumSha256(contentRef),
            dbDriver,
            transObjFactory,
            transMgrProvider
        );

        dbDriver.create(extFile);

        return extFile;
    }
}
