package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.RscConnPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.identifier.NodeName;
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
public class ResourceConnection extends AbsCoreObj<ResourceConnection>
{
    private final Resource source;
    private final Resource target;
    private final ResourceConnectionKey connectionKey;

    private final Props props;

    // State flags
    private final StateFlags<Flags> flags;

    // TCP Ports
    private final TransactionSimpleObject<ResourceConnection, TcpPortNumber> drbdProxyPortSource;
    private final TransactionSimpleObject<ResourceConnection, TcpPortNumber> drbdProxyPortTarget;

    private final ResourceConnectionDatabaseDriver dbDriver;

    /**
     * Use ResourceConnection.createWithSorting instead
     */
    private ResourceConnection(
        UUID uuid,
        Resource sourceResourceRef,
        Resource targetResourceRef,
        @Nullable TcpPortNumber drbdProxyPortSourceRef,
        @Nullable TcpPortNumber drbdProxyPortTargetRef,
        ResourceConnectionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        long initFlags
    )
        throws DatabaseException
    {
        super(uuid, transObjFactory, transMgrProviderRef);
        dbDriver = dbDriverRef;

        source = sourceResourceRef;
        target = targetResourceRef;

        connectionKey = new ResourceConnectionKey(sourceResourceRef, targetResourceRef);

        NodeName sourceNodeName = connectionKey.getSourceNodeName();
        NodeName targetNodeName = connectionKey.getTargetNodeName();
        if (!sourceResourceRef.getResourceDefinition().equals(targetResourceRef.getResourceDefinition()))
        {
            throw new ImplementationError(
                String.format(
                    "Creating connection between unrelated Resources %n" +
                        "Volume1: NodeName=%s, ResName=%s %n" +
                        "Volume2: NodeName=%s, ResName=%s.",
                        sourceNodeName.value,
                        sourceResourceRef.getResourceDefinition().getName().value,
                        targetNodeName.value,
                        targetResourceRef.getResourceDefinition().getName().value
                    ),
                null
            );
        }

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                connectionKey.getSourceNodeName(),
                connectionKey.getTargetNodeName(),
                sourceResourceRef.getResourceDefinition().getName()
            ),
            toStringImpl(),
            LinStorObject.RSC_CONN
        );

        flags = transObjFactory.createStateFlagsImpl(
            Arrays.asList(source.getObjProt(), target.getObjProt()),
            this,
            Flags.class,
            dbDriver.getStateFlagPersistence(),
            initFlags
        );

        drbdProxyPortSource = transObjFactory.createTransactionSimpleObject(
            this,
            drbdProxyPortSourceRef,
            this.dbDriver.getDrbdProxyPortSourceDriver()
        );
        drbdProxyPortTarget = transObjFactory.createTransactionSimpleObject(
            this,
            drbdProxyPortTargetRef,
            this.dbDriver.getDrbdProxyPortTargetDriver()
        );

        transObjs = Arrays.asList(
            source,
            target,
            flags,
            props,
            drbdProxyPortSource,
            drbdProxyPortTarget,
            deleted
        );
    }

    public static ResourceConnection createWithSorting(
        UUID uuidRef,
        Resource rsc1,
        Resource rsc2,
        @Nullable TcpPortNumber drbdProxyPort1Ref,
        @Nullable TcpPortNumber drbdProxyPort2Ref,
        ResourceConnectionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        long initFlags,
        AccessContext accCtx
    ) throws AccessDeniedException, LinStorDataAlreadyExistsException, DatabaseException
    {
        rsc1.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        rsc2.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        ResourceConnection rsc1ConData = rsc1.getAbsResourceConnection(accCtx, rsc2);
        ResourceConnection rsc2ConData = rsc2.getAbsResourceConnection(accCtx, rsc1);

        if (rsc1ConData != null || rsc2ConData != null)
        {
            if (rsc1ConData != null && rsc2ConData != null)
            {
                throw new LinStorDataAlreadyExistsException("The ResourceConnection already exists");
            }
            throw new LinStorDataAlreadyExistsException(
                "The ResourceConnection already exists for one of the resources"
            );
        }

        Resource src;
        Resource dst;
        @Nullable TcpPortNumber srcPort;
        @Nullable TcpPortNumber dstPort;
        int comp = rsc1.compareTo(rsc2);
        if (comp > 0)
        {
            src = rsc2;
            srcPort = drbdProxyPort2Ref;
            dst = rsc1;
            dstPort = drbdProxyPort1Ref;
        }
        else if (comp < 0)
        {
            src = rsc1;
            srcPort = drbdProxyPort1Ref;
            dst = rsc2;
            dstPort = drbdProxyPort2Ref;
        }
        else
        {
            throw new ImplementationError("Cannot create a resource connection to the same resource");
        }

        return createForDb(
            uuidRef,
            src,
            dst,
            srcPort,
            dstPort,
            dbDriverRef,
            propsContainerFactory,
            transObjFactory,
            transMgrProviderRef,
            initFlags
        );
    }

    /**
     * WARNING: do not use this method unless you are absolutely sure the resourceConnection you are trying to create
     * does not exist yet and the resources are already sorted correctly.
     * If you are not sure they are, use ResourceConnection.createWithSorting instead.
     */
    public static ResourceConnection createForDb(
        UUID uuidRef,
        Resource rsc1,
        Resource rsc2,
        @Nullable TcpPortNumber drbdProxyPortSourceRef,
        @Nullable TcpPortNumber drbdProxyPortTargetRef,
        ResourceConnectionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        long initFlags
    ) throws DatabaseException
    {
        return new ResourceConnection(
            uuidRef,
            rsc1,
            rsc2,
            drbdProxyPortSourceRef,
            drbdProxyPortTargetRef,
            dbDriverRef,
            propsContainerFactory,
            transObjFactory,
            transMgrProviderRef,
            initFlags
        );
    }

    public @Nullable Node getNode(NodeName nodeName)
    {
        checkDeleted();
        Node node = null;
        if (connectionKey.getSourceNodeName().equals(nodeName))
        {
            node = source.getNode();
        }
        else
        if (connectionKey.getTargetNodeName().equals(nodeName))
        {
            node = target.getNode();
        }
        return node;
    }

    public Resource getSourceResource(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        requireAccess(accCtx, AccessType.VIEW);
        return source;
    }

    public Resource getTargetResource(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        requireAccess(accCtx, AccessType.VIEW);
        return target;
    }

    public Props getProps(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(
            accCtx,
            source.getObjProt(),
            target.getObjProt(),
            props
        );
    }

    public StateFlags<Flags> getStateFlags()
    {
        checkDeleted();
        return flags;
    }

    public @Nullable TcpPortNumber getDrbdProxyPortSource(AccessContext accCtx) throws AccessDeniedException
    {
        requireAccess(accCtx, AccessType.VIEW);
        return drbdProxyPortSource.get();
    }

    public @Nullable TcpPortNumber getDrbdProxyPortTarget(AccessContext accCtx) throws AccessDeniedException
    {
        requireAccess(accCtx, AccessType.VIEW);
        return drbdProxyPortTarget.get();
    }

    public @Nullable TcpPortNumber setDrbdProxyPortSource(AccessContext accCtx, @Nullable TcpPortNumber portNr)
        throws DatabaseException, AccessDeniedException, ValueInUseException
    {
        return setDrbdProxyPortImpl(accCtx, portNr, source);
    }

    public @Nullable TcpPortNumber setDrbdProxyPortTarget(AccessContext accCtx, @Nullable TcpPortNumber portNr)
        throws DatabaseException, AccessDeniedException, ValueInUseException
    {
        return setDrbdProxyPortImpl(accCtx, portNr, target);
    }

    private @Nullable TcpPortNumber setDrbdProxyPortImpl(
        AccessContext accCtx,
        @Nullable TcpPortNumber portNr,
        Resource rsc
    )
        throws DatabaseException, ValueInUseException, AccessDeniedException
    {
        requireAccess(accCtx, AccessType.USE);
        DynamicNumberPool pool = rsc.getNode().getTcpPortPool(accCtx);
        @Nullable TcpPortNumber tcpPortNumber = drbdProxyPortSource.get();
        if (tcpPortNumber != null)
        {
            pool.deallocate(tcpPortNumber.value);
        }
        if (portNr != null)
        {
            pool.allocate(portNr.value);
        }
        return drbdProxyPortSource.set(portNr);
    }

    public void autoAllocateDrbdProxyPortSource(AccessContext accCtx)
        throws DatabaseException, AccessDeniedException, ExhaustedPoolException
    {
        autoAllocateDrbdProxyPortImpl(accCtx, drbdProxyPortSource, source);
    }

    public void autoAllocateDrbdProxyPortTarget(AccessContext accCtx)
        throws DatabaseException, AccessDeniedException, ExhaustedPoolException
    {
        autoAllocateDrbdProxyPortImpl(accCtx, drbdProxyPortTarget, target);
    }

    private void autoAllocateDrbdProxyPortImpl(
        AccessContext accCtxRef,
        TransactionSimpleObject<ResourceConnection, TcpPortNumber> drbdProxyPortFieldRef,
        Resource rscRef
    )
        throws AccessDeniedException, ExhaustedPoolException, DatabaseException
    {
        checkDeleted();
        requireAccess(accCtxRef, AccessType.USE);
        DynamicNumberPool tcpPortPool = rscRef.getNode().getTcpPortPool(accCtxRef);
        @Nullable TcpPortNumber tcpPortNumber = drbdProxyPortFieldRef.get();
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
        drbdProxyPortFieldRef.set(portNr);
    }

    private void requireAccess(AccessContext accCtxRef, AccessType accType) throws AccessDeniedException
    {
        source.getObjProt().requireAccess(accCtxRef, accType);
        target.getObjProt().requireAccess(accCtxRef, accType);
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            requireAccess(accCtx, AccessType.CHANGE);

            source.removeResourceConnection(accCtx, this);
            target.removeResourceConnection(accCtx, this);

            props.delete();

            @Nullable TcpPortNumber srcPort = drbdProxyPortSource.get();
            if (srcPort != null)
            {
                source.getNode().getTcpPortPool(accCtx).deallocate(srcPort.value);
            }
            @Nullable TcpPortNumber dstPort = drbdProxyPortTarget.get();
            if (dstPort != null)
            {
                target.getNode().getTcpPortPool(accCtx).deallocate(dstPort.value);
            }

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(Boolean.TRUE);
        }
    }

    private ResourceConnectionKey getConnectionKey()
    {
        checkDeleted();
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
    public String toStringImpl()
    {
        return "Node1: '" + connectionKey.getSourceNodeName() + "', " +
            "Node2: '" + connectionKey.getTargetNodeName() + "', " +
            "Rsc: '" + connectionKey.getResourceName() + "'";
    }

    public ResourceConnectionApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return new RscConnPojo(
            getUuid(),
            connectionKey.getSourceNodeName().getDisplayName(),
            connectionKey.getTargetNodeName().getDisplayName(),
            connectionKey.getResourceName().getDisplayName(),
            getProps(accCtx).map(),
            getStateFlags().getFlagsBits(accCtx),
            TcpPortNumber.getValueNullable(getDrbdProxyPortSource(accCtx)),
            TcpPortNumber.getValueNullable(getDrbdProxyPortTarget(accCtx))
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
