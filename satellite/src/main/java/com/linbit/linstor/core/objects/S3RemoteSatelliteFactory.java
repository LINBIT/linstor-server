package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.CriticalError;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.remotes.S3RemoteDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.UUID;

public class S3RemoteSatelliteFactory
{
    private final ErrorReporter errorReporter;
    private final S3RemoteDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final RemoteMap remoteMap;

    @Inject
    public S3RemoteSatelliteFactory(
        ErrorReporter errorReporterRef,
        CoreModule.RemoteMap remoteMapRef,
        S3RemoteDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        remoteMap = remoteMapRef;
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public S3Remote getInstanceSatellite(
        AccessContext accCtx,
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
            try
            {
                s3remote = new S3Remote(
                    objectProtectionFactory.getInstance(accCtx, "", true),
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
            catch (DatabaseException exc)
            {
                throw new ImplementationError(exc);
            }
        }
        else
        {
            if (!remote.getUuid().equals(uuid))
            {
                CriticalError.dieUuidMissmatch(
                    errorReporter,
                    S3Remote.class.getSimpleName(),
                    remote.getName().displayValue,
                    remoteNameRef.displayValue,
                    remote.getUuid(),
                    uuid
                );
            }
            if (remote instanceof S3Remote)
            {
                s3remote = (S3Remote) remote;
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
