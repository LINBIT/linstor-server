package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.UUID;

public class ResourceConnectionDataFactory
{
    private final ResourceConnectionDataDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;

    @Inject
    public ResourceConnectionDataFactory(
        ResourceConnectionDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef
    )
    {
        dbDriver = dbDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
    }

    public ResourceConnectionData getInstance(
        AccessContext accCtx,
        Resource sourceResource,
        Resource targetResource,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws AccessDeniedException, SQLException, LinStorDataAlreadyExistsException
    {
        ResourceConnectionData rscConData = null;

        Resource source;
        Resource target;

        NodeName sourceNodeName = sourceResource.getAssignedNode().getName();
        NodeName targetNodeName = targetResource.getAssignedNode().getName();

        if (sourceNodeName.compareTo(targetNodeName) < 0)
        {
            source = sourceResource;
            target = targetResource;
        }
        else
        {
            source = targetResource;
            target = sourceResource;
        }
        source.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        target.getObjProt().requireAccess(accCtx, AccessType.CHANGE);


        rscConData = dbDriver.load(
            source,
            target,
            false,
            transMgr
        );

        if (failIfExists && rscConData != null)
        {
            throw new LinStorDataAlreadyExistsException("The ResourceConnection already exists");
        }

        if (rscConData == null && createIfNotExists)
        {
            rscConData = new ResourceConnectionData(
                UUID.randomUUID(),
                accCtx,
                source,
                target,
                transMgr,
                dbDriver,
                propsContainerFactory
            );
            dbDriver.create(rscConData, transMgr);
        }
        if (rscConData != null)
        {
            rscConData.initialized();
            rscConData.setConnection(transMgr);
        }
        return rscConData;
    }

    public ResourceConnectionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Resource sourceResource,
        Resource targetResource,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        ResourceConnectionData rscConData = null;
        Resource source;
        Resource target;

        NodeName sourceNodeName = sourceResource.getAssignedNode().getName();
        NodeName targetNodeName = targetResource.getAssignedNode().getName();

        if (sourceNodeName.compareTo(targetNodeName) < 0)
        {
            source = sourceResource;
            target = targetResource;
        }
        else
        {
            source = targetResource;
            target = sourceResource;
        }

        try
        {
            rscConData = dbDriver.load(
                source,
                target,
                false,
                transMgr
            );

            if (rscConData == null)
            {
                rscConData = new ResourceConnectionData(
                    uuid,
                    accCtx,
                    source,
                    target,
                    transMgr,
                    dbDriver,
                    propsContainerFactory
                );
            }
            rscConData.initialized();
            rscConData.setConnection(transMgr);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return rscConData;
    }
}
