package com.linbit.linstor.core.objects.remotes;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.noop.NoOpFlagDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.UUID;

@Singleton
public class StltRemoteControllerFactory
{
    private final ObjectProtectionFactory objProtFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final RemoteRepository remoteRepo;
    private final StateFlagsPersistence<?> stateFlagsDriver = new NoOpFlagDriver();

    @Inject
    public StltRemoteControllerFactory(
        ObjectProtectionFactory objProtFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        RemoteRepository extFileRepoRef
    )
    {
        objProtFactory = objProtFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        remoteRepo = extFileRepoRef;
    }

    public StltRemote create(
        AccessContext accCtxRef,
        RemoteName nameRef,
        String ipRef,
        Map<String, Integer> portsRef,
        RemoteName linstorRemoteNameRef,
        Node nodeRef,
        String otherRscNameRef
    )
        throws AccessDeniedException, DatabaseException
    {
        if (remoteRepo.get(accCtxRef, nameRef) != null)
        {
            throw new ImplementationError("This remote name is already registered");
        }

        return new StltRemote(
            objProtFactory.getInstance(
                accCtxRef,
                ObjectProtection.buildPath(nameRef),
                true
            ),
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
