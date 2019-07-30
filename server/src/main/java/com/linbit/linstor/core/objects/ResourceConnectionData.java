package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.api.pojo.RscConnPojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
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

import javax.inject.Provider;
import java.util.Arrays;
import java.util.UUID;

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

    // TCP Port
    private final TransactionSimpleObject<ResourceConnectionData, TcpPortNumber> port;

    private final DynamicNumberPool tcpPortPool;

    private final ResourceConnectionDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<ResourceConnectionData, Boolean> deleted;

    ResourceConnectionData(
        UUID uuid,
        Resource sourceResourceRef,
        Resource targetResourceRef,
        TcpPortNumber portRef,
        DynamicNumberPool tcpPortPoolRef,
        ResourceConnectionDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        long initFlags
    )
        throws DatabaseException
    {
        super(transMgrProviderRef);
        tcpPortPool = tcpPortPoolRef;
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

        port = transObjFactory.createTransactionSimpleObject(
            this,
            portRef,
            this.dbDriver.getPortDriver()
        );

        transObjs = Arrays.asList(
            connectionKey.getSource(),
            connectionKey.getTarget(),
            flags,
            props,
            port,
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
    public Node getNode(NodeName nodeName)
    {
        checkDeleted();
        Node node = null;
        if (connectionKey.getSource().getAssignedNode().getName().equals(nodeName))
        {
            node = connectionKey.getSource().getAssignedNode();
        }
        else if (connectionKey.getTarget().getAssignedNode().getName().equals(nodeName))
        {
            node = connectionKey.getTarget().getAssignedNode();
        }
        return node;
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
    public TcpPortNumber getPort(AccessContext accCtx)
    {
        return port.get();
    }

    @Override
    public TcpPortNumber setPort(AccessContext accCtx, TcpPortNumber portNr)
        throws DatabaseException, ValueInUseException
    {
        if (tcpPortPool != null)
        {
            TcpPortNumber tcpPortNumber = port.get();
            if (tcpPortNumber != null)
            {
                tcpPortPool.deallocate(tcpPortNumber.value);
            }
            if (portNr != null)
            {
                tcpPortPool.allocate(portNr.value);
            }
        }
        return port.set(portNr);
    }

    @Override
    public void autoAllocatePort(AccessContext accCtx)
        throws DatabaseException, ExhaustedPoolException
    {
        TcpPortNumber tcpPortNumber = port.get();
        if (tcpPortNumber != null)
        {
            tcpPortPool.deallocate(tcpPortNumber.value);
        }

        TcpPortNumber portNr;
        try
        {
            portNr = new TcpPortNumber(tcpPortPool.autoAllocate());
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new ImplementationError("Auto-allocated TCP port number out of range", exc);
        }

        port.set(portNr);
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            connectionKey.getSource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
            connectionKey.getTarget().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

            connectionKey.getSource().removeResourceConnection(accCtx, this);
            connectionKey.getTarget().removeResourceConnection(accCtx, this);

            props.delete();

            if (tcpPortPool != null && port.get() != null)
            {
                tcpPortPool.deallocate(port.get().value);
            }

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
            getStateFlags().getFlagsBits(accCtx),
            TcpPortNumber.getValueNullable(getPort(accCtx))
        );
    }
}
