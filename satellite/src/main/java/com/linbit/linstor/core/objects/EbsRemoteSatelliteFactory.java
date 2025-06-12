package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.CriticalError;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.remotes.EbsRemoteDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.net.URL;
import java.util.UUID;

public class EbsRemoteSatelliteFactory
{
    private final ErrorReporter errorReporter;
    private final EbsRemoteDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final RemoteMap remoteMap;

    @Inject
    public EbsRemoteSatelliteFactory(
        ErrorReporter errorReporterRef,
        CoreModule.RemoteMap remoteMapRef,
        EbsRemoteDatabaseDriver driverRef,
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

    public EbsRemote getInstanceSatellite(
        AccessContext accCtx,
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
            try
            {
                ebsRemote = new EbsRemote(
                    objectProtectionFactory.getInstance(accCtx, "", true),
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
            if (remote instanceof EbsRemote)
            {
                ebsRemote = (EbsRemote) remote;
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
