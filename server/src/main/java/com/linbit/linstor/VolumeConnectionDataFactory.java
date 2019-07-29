package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.UUID;

public class VolumeConnectionDataFactory
{
    private final VolumeConnectionDataDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public VolumeConnectionDataFactory(
        VolumeConnectionDataDatabaseDriver dbDriverRef,
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

    public VolumeConnectionData create(
        AccessContext accCtx,
        Volume sourceVolume,
        Volume targetVolume
    )
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException
    {
        sourceVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        VolumeConnectionData volConData = VolumeConnectionData.get(accCtx, sourceVolume, targetVolume);

        if (volConData != null)
        {
            throw new LinStorDataAlreadyExistsException("The VolumeConnection already exists");
        }

        volConData = new VolumeConnectionData(
            UUID.randomUUID(),
            sourceVolume,
            targetVolume,
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );

        dbDriver.create(volConData);
        sourceVolume.setVolumeConnection(accCtx, volConData);
        targetVolume.setVolumeConnection(accCtx, volConData);

        return volConData;
    }

    public VolumeConnectionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Volume sourceVolume,
        Volume targetVolume
    )
        throws ImplementationError
    {
        VolumeConnectionData volConData = null;

        Volume source;
        Volume target;
        NodeName sourceNodeName = sourceVolume.getResource().getAssignedNode().getName();
        NodeName targetNodeName = targetVolume.getResource().getAssignedNode().getName();
        if (sourceNodeName.compareTo(targetNodeName) < 0)
        {
            source = sourceVolume;
            target = targetVolume;
        }
        else
        {
            source = targetVolume;
            target = sourceVolume;
        }

        try
        {
            volConData = (VolumeConnectionData) source.getVolumeConnection(accCtx, target);
            if (volConData == null)
            {
                volConData = new VolumeConnectionData(
                    uuid,
                    source,
                    target,
                    dbDriver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
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
