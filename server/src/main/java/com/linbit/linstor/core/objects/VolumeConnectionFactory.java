package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.UUID;

public class VolumeConnectionFactory
{
    private final VolumeConnectionDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public VolumeConnectionFactory(
        VolumeConnectionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbDriver = dbDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public VolumeConnection create(
        AccessContext accCtx,
        Volume sourceVolume,
        Volume targetVolume
    )
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException
    {
        sourceVolume.getAbsResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetVolume.getAbsResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        VolumeConnection volConData = VolumeConnection.createWithSorting(
            UUID.randomUUID(),
            sourceVolume,
            targetVolume,
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            accCtx
        );

        dbDriver.create(volConData);
        sourceVolume.setVolumeConnection(accCtx, volConData);
        targetVolume.setVolumeConnection(accCtx, volConData);

        return volConData;
    }

    public VolumeConnection getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Volume sourceVolume,
        Volume targetVolume
    )
        throws ImplementationError
    {
        VolumeConnection volConData = null;

        try
        {
            volConData = sourceVolume.getVolumeConnection(accCtx, targetVolume);
            if (volConData == null)
            {
                volConData = VolumeConnection.createWithSorting(
                    uuid,
                    sourceVolume,
                    targetVolume,
                    dbDriver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    accCtx
                );
                sourceVolume.setVolumeConnection(accCtx, volConData);
                targetVolume.setVolumeConnection(accCtx, volConData);
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }

        return volConData;
    }
}
