package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.CriticalError;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.dbdrivers.noop.NoOpFlagDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.Map;
import java.util.UUID;

public class StltRemoteSatelliteFactory
{
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final RemoteMap remoteMap;
    private final StateFlagsPersistence<?> noopFlagDriver = new NoOpFlagDriver();

    @Inject
    public StltRemoteSatelliteFactory(
        CoreModule.RemoteMap remoteMapRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        remoteMap = remoteMapRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public StltRemote getInstanceSatellite(
        UUID uuid,
        RemoteName remoteNameRef,
        RemoteName linRemoteNameRef,
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
            stltRemote = new StltRemote(
                uuid,
                remoteNameRef,
                initflags,
                ipRef,
                portsRef,
                useZstdRef,
                linRemoteNameRef,
                nodeRef,
                (StateFlagsPersistence<StltRemote>) noopFlagDriver,
                transObjFactory,
                transMgrProvider,
                null
            );
            remoteMap.put(remoteNameRef, stltRemote);
        }
        else
        {
            if (!remote.getUuid().equals(uuid))
            {
                CriticalError.dieUuidMissmatch(
                    StltRemote.class.getSimpleName(),
                    remote.getName().displayValue,
                    remoteNameRef.displayValue,
                    remote.getUuid(),
                    uuid
                );
            }
            if (remote instanceof StltRemote stltRmt)
            {
                stltRemote = stltRmt;
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
