package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;

/**
 * Defines a connection between two DRBD resources
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceConnectionData extends BaseTransactionObject implements ResourceConnection
{
    // Object identifier
    private final UUID objId;
    private final Resource sourceResource;
    private final Resource targetResource;

    private final Props props;

    private final ResourceConnectionDataDatabaseDriver dbDriver;

    private boolean deleted = false;

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

        dbDriver = DrbdManage.getResourceConnectionDatabaseDriver();

        transObjs = Arrays.asList(
            sourceResource,
            targetResource,
            props
        );

        sourceResource.setResourceConnection(accCtx, this);
        targetResource.setResourceConnection(accCtx, this);
    }

    public static ResourceConnectionData getInstance(
        AccessContext accCtx,
        Resource sourceResource,
        Resource targetResource,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws AccessDeniedException, SQLException, DrbdDataAlreadyExistsException
    {
        ResourceConnectionData resConData = null;

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

        ResourceConnectionDataDatabaseDriver dbDriver = DrbdManage.getResourceConnectionDatabaseDriver();

        resConData = dbDriver.load(
            source,
            target,
            false,
            transMgr
        );

        if (failIfExists && resConData != null)
        {
            throw new DrbdDataAlreadyExistsException("The ResourceConnection already exists");
        }

        if (resConData == null && createIfNotExists)
        {
            resConData = new ResourceConnectionData(
                accCtx,
                source,
                target,
                transMgr
            );
            dbDriver.create(resConData, transMgr);
        }
        if (resConData != null)
        {
            resConData.initialized();
        }
        return resConData;
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
        checkDeleted();
        sourceResource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetResource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        sourceResource.removeResourceConnection(accCtx, this);
        targetResource.removeResourceConnection(accCtx, this);

        dbDriver.delete(this, transMgr);
        deleted = true;
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
        if (deleted)
        {
            throw new ImplementationError("Access to deleted ResourceConnection", null);
        }
    }
}
