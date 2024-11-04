package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ExternalFileMap;
import com.linbit.linstor.core.DivergentUuidsException;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ExternalFileDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.ByteUtils;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.UUID;

public class ExternalFileSatelliteFactory
{
    private final ExternalFileDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final ExternalFileMap externalFileMap;

    @Inject
    public ExternalFileSatelliteFactory(
        CoreModule.ExternalFileMap externalFileMapRef,
        ExternalFileDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        externalFileMap = externalFileMapRef;
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public ExternalFile getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        ExternalFileName extFileNameRef,
        long initflags,
        @Nullable byte[] content
    )
        throws ImplementationError
    {
        ExternalFile extFile = externalFileMap.get(extFileNameRef);
        if (extFile == null)
        {
            try
            {
                extFile = new ExternalFile(
                    uuid,
                    objectProtectionFactory.getInstance(accCtx, "", true),
                    extFileNameRef,
                    initflags,
                    content == null ? new byte[0] : content,
                    content == null ? new byte[0] : ByteUtils.checksumSha256(content),
                    driver,
                    transObjFactory,
                    transMgrProvider
                );
                externalFileMap.put(extFileNameRef, extFile);
            }
            catch (DatabaseException exc)
            {
                throw new ImplementationError(exc);
            }
        }
        else
        {
            if (!extFile.getUuid().equals(uuid))
            {
                throw new DivergentUuidsException(
                    ExternalFile.class.getSimpleName(),
                    extFile.getName().extFileName,
                    extFileNameRef.extFileName,
                    extFile.getUuid(),
                    uuid
                );
            }
        }
        return extFile;
    }
}
