package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.UUID;

public class VolumeConnectionDataFactory
{
    private final VolumeConnectionDataDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;

    @Inject
    public VolumeConnectionDataFactory(
        VolumeConnectionDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef
    )
    {
        dbDriver = dbDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
    }

    public VolumeConnectionData getInstance(
        AccessContext accCtx,
        Volume sourceVolume,
        Volume targetVolume,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws AccessDeniedException, SQLException, LinStorDataAlreadyExistsException
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
        source.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        target.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);


        volConData = dbDriver.load(
            source,
            target,
            false,
            transMgr
        );

        if (failIfExists && volConData != null)
        {
            throw new LinStorDataAlreadyExistsException("The VolumeConnection already exists");
        }

        if (volConData == null && createIfNotExists)
        {
            volConData = new VolumeConnectionData(
                UUID.randomUUID(),
                accCtx,
                source,
                target,
                transMgr,
                dbDriver,
                propsContainerFactory
            );

            dbDriver.create(volConData, transMgr);
        }
        if (volConData != null)
        {
            volConData.initialized();
            volConData.setConnection(transMgr);
        }
        return volConData;
    }

    public VolumeConnectionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Volume sourceVolume,
        Volume targetVolume,
        SatelliteTransactionMgr transMgr
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
            volConData = dbDriver.load(
                source,
                target,
                false,
                transMgr
            );
            if (volConData == null)
            {
                volConData = new VolumeConnectionData(
                    uuid,
                    accCtx,
                    source,
                    target,
                    transMgr,
                    dbDriver,
                    propsContainerFactory
                );
            }
            volConData.initialized();
            volConData.setConnection(transMgr);
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
