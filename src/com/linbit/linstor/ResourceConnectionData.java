package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.pojo.RscConnPojo;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import javax.inject.Provider;

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

    ResourceConnectionData(
        UUID uuid,
        Resource sourceResourceRef,
        Resource targetResourceRef,
        ResourceConnectionDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef
    )
        throws SQLException
    {
        super(transMgrProviderRef);
        dbDriver = dbDriverRef;

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

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                sourceNodeName,
                targetNodeName,
                sourceResourceRef.getDefinition().getName()
            )
        );

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            sourceResource,
            targetResource,
            props,
            deleted
        );
    }

    public static ResourceConnectionData get(
        AccessContext accCtx,
        Resource sourceResource,
        Resource targetResource
    )
        throws AccessDeniedException
    {
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

        return (ResourceConnectionData) sourceResource.getResourceConnection(accCtx, targetResource);
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
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

            props.delete();

            activateTransMgr();
            dbDriver.delete(this);

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

    @Override
    public RscConnApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        return new RscConnPojo(
            getUuid(),
            sourceResource.getAssignedNode().getName().getDisplayName(),
            targetResource.getAssignedNode().getName().getDisplayName(),
            getProps(accCtx).map()
        );
    }
}
