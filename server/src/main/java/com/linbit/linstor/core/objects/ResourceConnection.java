package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.pojo.RscConnPojo;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Defines a connection between two LinStor resources
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceConnection extends BaseTransactionObject
    implements DbgInstanceUuid, Comparable<ResourceConnection>
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final ResourceConnectionKey connectionKey;

    private final Props props;

    // State flags
    private final StateFlags<Flags> flags;

    // TCP Port
    private final TransactionSimpleObject<ResourceConnection, TcpPortNumber> port;

    private final DynamicNumberPool tcpPortPool;

    private final ResourceConnectionDatabaseDriver dbDriver;

    private final TransactionSimpleObject<ResourceConnection, Boolean> deleted;

    private final TransactionSimpleObject<ResourceConnection, SnapshotName> snapshotNameInProgress;

    ResourceConnection(
        UUID uuid,
        Resource sourceResourceRef,
        Resource targetResourceRef,
        TcpPortNumber portRef,
        DynamicNumberPool tcpPortPoolRef,
        ResourceConnectionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        long initFlags
    )
        throws DatabaseException
    {
        super(transMgrProviderRef);
        tcpPortPool = tcpPortPoolRef;
        dbDriver = dbDriverRef;

        connectionKey = new ResourceConnectionKey(sourceResourceRef, targetResourceRef);

        NodeName sourceNodeName = connectionKey.getSource().getNode().getName();
        NodeName targetNodeName = connectionKey.getTarget().getNode().getName();
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
                connectionKey.getSource().getNode().getName(),
                connectionKey.getTarget().getNode().getName(),
                sourceResourceRef.getDefinition().getName()
            )
        );

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        flags = transObjFactory.createStateFlagsImpl(
            Arrays.asList(connectionKey.getSource().getObjProt(), connectionKey.getTarget().getObjProt()),
            this,
            Flags.class,
            dbDriver.getStateFlagPersistence(),
            initFlags
        );

        port = transObjFactory.createTransactionSimpleObject(
            this,
            portRef,
            this.dbDriver.getPortDriver()
        );
        snapshotNameInProgress = transObjFactory.createTransactionSimpleObject(this, null, null);

        transObjs = Arrays.asList(
            connectionKey.getSource(),
            connectionKey.getTarget(),
            flags,
            props,
            port,
            snapshotNameInProgress,
            deleted
        );
    }

    public static ResourceConnection get(
        AccessContext accCtx,
        Resource sourceResource,
        Resource targetResource
    )
        throws AccessDeniedException
    {
        return sourceResource.getAbsResourceConnection(accCtx, targetResource);
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    public Node getNode(NodeName nodeName)
    {
        checkDeleted();
        Node node = null;
        if (connectionKey.getSource().getNode().getName().equals(nodeName))
        {
            node = connectionKey.getSource().getNode();
        }
        else
        if (connectionKey.getTarget().getNode().getName().equals(nodeName))
        {
            node = connectionKey.getTarget().getNode();
        }
        return node;
    }

    public Resource getSourceResource(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        requireAccess(accCtx, AccessType.VIEW);
        return connectionKey.getSource();
    }

    public Resource getTargetResource(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        requireAccess(accCtx, AccessType.VIEW);
        return connectionKey.getTarget();
    }

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

    public StateFlags<Flags> getStateFlags()
    {
        checkDeleted();
        return flags;
    }

    public TcpPortNumber getPort(AccessContext accCtx) throws AccessDeniedException
    {
        requireAccess(accCtx, AccessType.VIEW);
        return port.get();
    }

    public TcpPortNumber setPort(AccessContext accCtx, TcpPortNumber portNr)
        throws DatabaseException, ValueInUseException, AccessDeniedException
    {
        requireAccess(accCtx, AccessType.USE);
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

    public void autoAllocatePort(AccessContext accCtx)
        throws DatabaseException, ExhaustedPoolException, AccessDeniedException
    {
        requireAccess(accCtx, AccessType.USE);
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

    public void setSnapshotShippingNameInProgress(SnapshotName snapshotNameInProgressRef) throws DatabaseException
    {
        snapshotNameInProgress.set(snapshotNameInProgressRef);
    }

    public SnapshotName getSnapshotShippingNameInProgress()
    {
        return snapshotNameInProgress.get();
    }

    private void requireAccess(AccessContext accCtxRef, AccessType accType) throws AccessDeniedException
    {
        connectionKey.getSource().getObjProt().requireAccess(accCtxRef, accType);
        connectionKey.getTarget().getObjProt().requireAccess(accCtxRef, accType);
    }

    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            requireAccess(accCtx, AccessType.CHANGE);

            connectionKey.getSource().removeResourceConnection(accCtx, this);
            connectionKey.getTarget().removeResourceConnection(accCtx, this);

            props.delete();

            if (tcpPortPool != null && port.get() != null)
            {
                tcpPortPool.deallocate(port.get().value);
            }

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(Boolean.TRUE);
        }
    }

    private ResourceConnectionKey getConnectionKey()
    {
        return connectionKey;
    }

    @Override
    public int compareTo(ResourceConnection other)
    {
        return connectionKey.compareTo(other.getConnectionKey());
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(connectionKey);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof ResourceConnection)
        {
            ResourceConnection other = (ResourceConnection) obj;
            other.checkDeleted();
            ret = Objects.equals(connectionKey, other.connectionKey);
        }
        return ret;
    }

    @Override
    public String toString()
    {
        return "Node1: '" + connectionKey.getSource().getNode().getName() + "', " +
               "Node2: '" + connectionKey.getTarget().getNode().getName() + "', " +
               "Rsc: '" + connectionKey.getSource().getDefinition().getName() + "'";
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted resource connection");
        }
    }

    public ResourceConnectionApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        return new RscConnPojo(
            getUuid(),
            connectionKey.getSource().getNode().getName().getDisplayName(),
            connectionKey.getTarget().getNode().getName().getDisplayName(),
            connectionKey.getSource().getDefinition().getName().getDisplayName(),
            getProps(accCtx).map(),
            getStateFlags().getFlagsBits(accCtx),
            TcpPortNumber.getValueNullable(getPort(accCtx))
        );
    }
    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        DELETED(1L << 0),
        LOCAL_DRBD_PROXY(1L << 1);

        public final long flagValue;

        Flags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static Flags[] restoreFlags(long rscFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((rscFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new Flags[flagList.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(Flags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(Flags.class, listFlags);
        }
    }
}
