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
        Resource sourceResource,
        Resource targetResource,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        this(
            UUID.randomUUID(),
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
        Resource sourceResourceRef,
        Resource targetResourceRef,
        TransactionMgr transMgr
    )
        throws SQLException
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
    }

    public static ResourceConnectionData getInstance(
        AccessContext accCtx,
        Resource sourceResource,
        Resource targetResource,
        TransactionMgr transMgr,
        boolean createIfNotExists
    )
        throws AccessDeniedException, SQLException
    {
        ResourceConnectionData resConDfnData = null;

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

        resConDfnData = dbDriver.load(
            source,
            target,
            false,
            transMgr
        );

        if (resConDfnData == null && createIfNotExists)
        {
            resConDfnData = new ResourceConnectionData(
                source,
                target,
                transMgr
            );
            dbDriver.create(resConDfnData, transMgr);
        }
        if (resConDfnData != null)
        {
            sourceResource.setResourceConnection(accCtx, resConDfnData);
            targetResource.setResourceConnection(accCtx, resConDfnData);

            resConDfnData.initialized();
        }
        return resConDfnData;
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

    private void checkDeleted()
    {
        if (deleted)
        {
            throw new ImplementationError("Access to deleted ResourceConnection", null);
        }
    }
}
