package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.TransactionSimpleObject;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

/**
 * Defines a connection between two LinStor resources
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceConnectionData extends BaseTransactionObject implements ResourceConnection
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final Resource sourceResource;
    private final Resource targetResource;

    private final Props props;

    private final ResourceConnectionDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<ResourceConnectionData, Boolean> deleted;

    /*
     * used by getInstance
     */
    private ResourceConnectionData(
        AccessContext accCtx,
        Resource sourceResource,
        Resource targetResource,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            accCtx,
            sourceResource,
            targetResource,
            transMgr
        );
    }

    /*
     * used by dbDrivers and tests
     */
    ResourceConnectionData(
        UUID uuid,
        AccessContext accCtx,
        Resource sourceResourceRef,
        Resource targetResourceRef,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        NodeName sourceNodeName = sourceResourceRef.getAssignedNode().getName();
        NodeName targetNodeName = targetResourceRef.getAssignedNode().getName();
        if (sourceResourceRef.getDefinition() != targetResourceRef.getDefinition())
        {
            throw new ImplementationError(
                String.format(
                    "Creating connection between unrelated Resources %n" +
                        "Volume1: NodeName=%s, ResName=%s %n" +
                        "Volume2: NodeName=%s, ResName=%s.",
                        sourceNodeName.value,
                        sourceResourceRef.getDefinition().getName().value,
                        targetNodeName.value,
                        targetResourceRef.getDefinition().getName().value
                    ),
                null
            );
        }

        objId = uuid;
        dbgInstanceId = UUID.randomUUID();

        if (sourceNodeName.getName().compareTo(targetNodeName.getName()) < 0)
        {
            sourceResource = sourceResourceRef;
            targetResource = targetResourceRef;
        }
        else
        {
            sourceResource = targetResourceRef;
            targetResource = sourceResourceRef;
        }

        props = PropsContainer.getInstance(
            PropsContainer.buildPath(
                sourceNodeName,
                targetNodeName,
                sourceResourceRef.getDefinition().getName()
            ),
            transMgr
        );

        deleted = new TransactionSimpleObject<>(this, false, null);

        dbDriver = LinStor.getResourceConnectionDatabaseDriver();

        transObjs = Arrays.asList(
            sourceResource,
            targetResource,
            props,
            deleted
        );

        sourceResource.setResourceConnection(accCtx, this);
        targetResource.setResourceConnection(accCtx, this);
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public static ResourceConnectionData getInstance(
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

        ResourceConnectionDataDatabaseDriver dbDriver = LinStor.getResourceConnectionDatabaseDriver();

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
                accCtx,
                source,
                target,
                transMgr
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

    public static ResourceConnectionData getInstanceSatellite(
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
        ResourceConnectionDataDatabaseDriver dbDriver = LinStor.getResourceConnectionDatabaseDriver();

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
                    transMgr
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

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public Resource getSourceResource(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return sourceResource;
    }

    @Override
    public Resource getTargetResource(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return targetResource;
    }

    @Override
    public Props getProps(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(
            accCtx,
            sourceResource.getObjProt(),
            targetResource.getObjProt(),
            props
        );
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            sourceResource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
            targetResource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

            sourceResource.removeResourceConnection(accCtx, this);
            targetResource.removeResourceConnection(accCtx, this);

            dbDriver.delete(this, transMgr);

            deleted.set(true);
        }
    }

    @Override
    public String toString()
    {
        return "Node1: '" + sourceResource.getAssignedNode().getName() + "', " +
               "Node2: '" + targetResource.getAssignedNode().getName() + "', " +
               "Rsc: '" + sourceResource.getDefinition().getName() + "'";
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted resource connection");
        }
    }
}
