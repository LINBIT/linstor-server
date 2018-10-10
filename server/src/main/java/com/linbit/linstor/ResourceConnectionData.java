package com.linbit.linstor;

import javax.inject.Provider;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

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
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

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

    private final ResourceConnectionKey connectionKey;

    private final Props props;

    // State flags
    private final StateFlags<ResourceConnection.RscConnFlags> flags;

    private final ResourceConnectionDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<ResourceConnectionData, Boolean> deleted;

    ResourceConnectionData(
        UUID uuid,
        Resource sourceResourceRef,
        Resource targetResourceRef,
        ResourceConnectionDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        long initFlags
    )
        throws SQLException
    {
        super(transMgrProviderRef);
        dbDriver = dbDriverRef;

        connectionKey = new ResourceConnectionKey(sourceResourceRef, targetResourceRef);

        NodeName sourceNodeName = connectionKey.getSource().getAssignedNode().getName();
        NodeName targetNodeName = connectionKey.getTarget().getAssignedNode().getName();
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

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                connectionKey.getSource().getAssignedNode().getName(),
                connectionKey.getTarget().getAssignedNode().getName(),
                sourceResourceRef.getDefinition().getName()
            )
        );

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        flags = transObjFactory.createStateFlagsImpl(
            Arrays.asList(connectionKey.getSource().getObjProt(), connectionKey.getTarget().getObjProt()),
            this,
            ResourceConnection.RscConnFlags.class,
            dbDriver.getStateFlagPersistence(),
            initFlags
        );

        transObjs = Arrays.asList(
            connectionKey.getSource(),
            connectionKey.getTarget(),
            flags,
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
        return connectionKey.getSource();
    }

    @Override
    public Resource getTargetResource(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return connectionKey.getTarget();
    }

    @Override
    public Props getProps(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(
            accCtx,
            connectionKey.getSource().getObjProt(),
            connectionKey.getTarget().getObjProt(),
            props
        );
    }

    @Override
    public StateFlags<ResourceConnection.RscConnFlags> getStateFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            connectionKey.getSource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
            connectionKey.getTarget().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

            connectionKey.getSource().removeResourceConnection(accCtx, this);
            connectionKey.getTarget().removeResourceConnection(accCtx, this);

            props.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public String toString()
    {
        return "Node1: '" + connectionKey.getSource().getAssignedNode().getName() + "', " +
               "Node2: '" + connectionKey.getTarget().getAssignedNode().getName() + "', " +
               "Rsc: '" + connectionKey.getSource().getDefinition().getName() + "'";
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
            connectionKey.getSource().getAssignedNode().getName().getDisplayName(),
            connectionKey.getTarget().getAssignedNode().getName().getDisplayName(),
            connectionKey.getSource().getDefinition().getName().getDisplayName(),
            getProps(accCtx).map(),
            getStateFlags().getFlagsBits(accCtx)
        );
    }
}
