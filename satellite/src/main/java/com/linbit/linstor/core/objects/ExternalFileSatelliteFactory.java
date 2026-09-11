package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ExternalFileMap;
import com.linbit.linstor.core.CriticalError;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.dbdrivers.interfaces.ExternalFileDatabaseDriver;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.ByteUtils;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ExternalFileSatelliteFactory
{
    private final ExternalFileDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final ExternalFileMap externalFileMap;

    @Inject
    public ExternalFileSatelliteFactory(
        CoreModule.ExternalFileMap externalFileMapRef,
        ExternalFileDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        externalFileMap = externalFileMapRef;
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public ExternalFile getInstanceSatellite(
        UUID uuid,
        ExternalFileName extFileNameRef,
        long initflags,
        @Nullable byte[] content,
        List<String> altSuffixesRef
    )
        throws ImplementationError
    {
        ExternalFile extFile = externalFileMap.get(extFileNameRef);
        if (extFile == null)
        {
            extFile = new ExternalFile(
                uuid,
                extFileNameRef,
                initflags,
                content == null ? new byte[0] : content,
                content == null ? new byte[0] : ByteUtils.checksumSha256(content),
                // not sure where the parameter comes from, so we make a copy of it, just to be sure
                new ArrayList<>(altSuffixesRef),
                driver,
                transObjFactory,
                transMgrProvider
            );
            externalFileMap.put(extFileNameRef, extFile);
        }
        else
        {
            if (!extFile.getUuid().equals(uuid))
            {
                CriticalError.dieUuidMissmatch(
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
