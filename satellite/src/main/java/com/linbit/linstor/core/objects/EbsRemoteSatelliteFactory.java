package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.CriticalError;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.dbdrivers.interfaces.remotes.EbsRemoteDatabaseDriver;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.net.URL;
import java.util.UUID;

public class EbsRemoteSatelliteFactory
{
    private final EbsRemoteDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final RemoteMap remoteMap;

    @Inject
    public EbsRemoteSatelliteFactory(
        CoreModule.RemoteMap remoteMapRef,
        EbsRemoteDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        remoteMap = remoteMapRef;
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public EbsRemote getInstanceSatellite(
        UUID uuid,
        RemoteName remoteNameRef,
        long initflags,
        URL endpointRef,
        String regionRef,
        String availabilityZoneRef,
        byte[] encryptedAccessKeyRef,
        byte[] encryptedSecretKeyRef
    )
        throws ImplementationError
    {
        AbsRemote remote = remoteMap.get(remoteNameRef);
        EbsRemote ebsRemote = null;
        if (remote == null)
        {
            ebsRemote = new EbsRemote(
                uuid,
                driver,
                remoteNameRef,
                initflags,
                endpointRef,
                regionRef,
                availabilityZoneRef,
                encryptedAccessKeyRef,
                encryptedSecretKeyRef,
                transObjFactory,
                transMgrProvider
            );
            remoteMap.put(remoteNameRef, ebsRemote);
        }
        else
        {
            if (!remote.getUuid().equals(uuid))
            {
                CriticalError.dieUuidMissmatch(
                    S3Remote.class.getSimpleName(),
                    remote.getName().displayValue,
                    remoteNameRef.displayValue,
                    remote.getUuid(),
                    uuid
                );
            }
            if (remote instanceof EbsRemote ebsRmt)
            {
                ebsRemote = ebsRmt;
            }
            else
            {
                throw new ImplementationError(
                    "Unknown implementation of Remote detected: " + remote.getClass().getCanonicalName()
                );
            }
        }
        return ebsRemote;
    }
}
