package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.CriticalError;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.dbdrivers.interfaces.remotes.S3RemoteDatabaseDriver;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.UUID;

public class S3RemoteSatelliteFactory
{
    private final S3RemoteDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final RemoteMap remoteMap;

    @Inject
    public S3RemoteSatelliteFactory(
        CoreModule.RemoteMap remoteMapRef,
        S3RemoteDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        remoteMap = remoteMapRef;
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public S3Remote getInstanceSatellite(
        UUID uuid,
        RemoteName remoteNameRef,
        long initflags,
        String endpointRef,
        String bucketRef,
        String regionRef,
        byte[] accessKeyRef,
        byte[] secretKeyRef
    )
        throws ImplementationError
    {
        AbsRemote remote = remoteMap.get(remoteNameRef);
        S3Remote s3remote = null;
        if (remote == null)
        {
            s3remote = new S3Remote(
                uuid,
                driver,
                remoteNameRef,
                initflags,
                endpointRef,
                bucketRef,
                regionRef,
                accessKeyRef,
                secretKeyRef,
                transObjFactory,
                transMgrProvider
            );
            remoteMap.put(remoteNameRef, s3remote);
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
            if (remote instanceof S3Remote s3rmt)
            {
                s3remote = s3rmt;
            }
            else
            {
                throw new ImplementationError(
                    "Unknown implementation of Remote detected: " + remote.getClass().getCanonicalName()
                );
            }
        }
        return s3remote;
    }
}
