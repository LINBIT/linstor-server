package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.UUID;

public class ResourceConnectionDataFactory
{
    private final ResourceConnectionDataDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceConnectionDataFactory(
        ResourceConnectionDataDatabaseDriver dbDriverRef,
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

    public ResourceConnectionData getInstance(
        AccessContext accCtx,
        Resource sourceResource,
        Resource targetResource,
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


        rscConData = dbDriver.load(source, target, false);

        if (failIfExists && rscConData != null)
        {
            throw new LinStorDataAlreadyExistsException("The ResourceConnection already exists");
        }

        if (rscConData == null && createIfNotExists)
        {
            rscConData = new ResourceConnectionData(
                UUID.randomUUID(),
                source,
                target,
                dbDriver,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );
            dbDriver.create(rscConData);

            sourceResource.setResourceConnection(accCtx, rscConData);
            targetResource.setResourceConnection(accCtx, rscConData);
        }
        return rscConData;
    }

    public ResourceConnectionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Resource sourceResource,
        Resource targetResource
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
            rscConData = dbDriver.load(source, target, false);

            if (rscConData == null)
            {
                rscConData = new ResourceConnectionData(
                    uuid,
                    source,
                    target,
                    dbDriver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                );
                sourceResource.setResourceConnection(accCtx, rscConData);
                targetResource.setResourceConnection(accCtx, rscConData);
            }
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
