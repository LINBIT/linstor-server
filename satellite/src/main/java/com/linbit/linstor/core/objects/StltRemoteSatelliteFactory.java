package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.DivergentUuidsException;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.noop.NoOpFlagDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Map;
import java.util.UUID;

public class StltRemoteSatelliteFactory
{
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final RemoteMap remoteMap;
    private final StateFlagsPersistence<?> noopFlagDriver = new NoOpFlagDriver();

    @Inject
    public StltRemoteSatelliteFactory(
        CoreModule.RemoteMap remoteMapRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        remoteMap = remoteMapRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public StltRemote getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        RemoteName remoteNameRef,
        @Nullable Node nodeRef,
        long initflags,
        String ipRef,
        Map<String, Integer> portsRef,
        Boolean useZstdRef
    )
        throws ImplementationError
    {
        AbsRemote remote = remoteMap.get(remoteNameRef);
        StltRemote stltRemote = null;
        if (remote == null)
        {
            try
            {
                stltRemote = new StltRemote(
                    objectProtectionFactory.getInstance(accCtx, "", true),
                    uuid,
                    remoteNameRef,
                    initflags,
                    ipRef,
                    portsRef,
                    useZstdRef,
                    null,
                    nodeRef,
                    (StateFlagsPersistence<StltRemote>) noopFlagDriver,
                    transObjFactory,
                    transMgrProvider,
                    null
                );
                remoteMap.put(remoteNameRef, stltRemote);
            }
            catch (DatabaseException exc)
            {
                throw new ImplementationError(exc);
            }
        }
        else
        {
            if (!remote.getUuid().equals(uuid))
            {
                throw new DivergentUuidsException(
                    StltRemote.class.getSimpleName(),
                    remote.getName().displayValue,
                    remoteNameRef.displayValue,
                    remote.getUuid(),
                    uuid
                );
            }
            if (remote instanceof StltRemote)
            {
                stltRemote = (StltRemote) remote;
            }
            else
            {
                throw new ImplementationError(
                    "Unknown implementation of Remote detected: " + remote.getClass().getCanonicalName()
                );
            }
        }
        return stltRemote;
    }
}
