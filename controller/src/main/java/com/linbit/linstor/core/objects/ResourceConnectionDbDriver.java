package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.AbsProtectedDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.core.objects.ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.NODE_NAME_DST;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.NODE_NAME_SRC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.TCP_PORT;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.UUID;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Objects;

@Singleton
public final class ResourceConnectionDbDriver
    extends AbsProtectedDatabaseDriver<ResourceConnection, Void, Map<Pair<NodeName, ResourceName>, ? extends Resource>>
    implements ResourceConnectionCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final DynamicNumberPool tcpPortPool;

    private final StateFlagsPersistence<ResourceConnection> flagsDriver;
    private final SingleColumnDatabaseDriver<ResourceConnection, TcpPortNumber> portDriver;

    @Inject
    public ResourceConnectionDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionFactory objProtFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.RESOURCE_CONNECTIONS, dbEngineRef, objProtFactoryRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        tcpPortPool = tcpPortPoolRef;

        setColumnSetter(UUID, rc -> rc.getUuid().toString());
        setColumnSetter(NODE_NAME_SRC, rc -> rc.getSourceResource(dbCtxRef).getNode().getName().value);
        setColumnSetter(NODE_NAME_DST, rc -> rc.getTargetResource(dbCtxRef).getNode().getName().value);
        setColumnSetter(RESOURCE_NAME, rc -> rc.getSourceResource(dbCtxRef).getResourceDefinition().getName().value);
        setColumnSetter(FLAGS, rc -> rc.getStateFlags().getFlagsBits(dbCtxRef));
        setColumnSetter(TCP_PORT, rc -> TcpPortNumber.getValueNullable(rc.getPort(dbCtxRef)));

        setColumnSetter(SNAPSHOT_NAME, ignored -> DFLT_SNAP_NAME_FOR_RSC);

        flagsDriver = generateFlagDriver(FLAGS, ResourceConnection.Flags.class);
        portDriver = generateSingleColumnDriver(
            TCP_PORT,
            rc -> Objects.toString(rc.getPort(dbCtxRef)),
            TcpPortNumber::getValueNullable
        );
    }

    @Override
    public StateFlagsPersistence<ResourceConnection> getStateFlagPersistence()
    {
        return flagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceConnection, TcpPortNumber> getPortDriver()
    {
        return portDriver;
    }

    @Override
    protected @Nullable Pair<ResourceConnection, Void> load(
        RawParameters raw,
        Map<Pair<NodeName, ResourceName>, ? extends Resource> rscMap
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final Pair<ResourceConnection, Void> ret;
        if (!raw.get(SNAPSHOT_NAME).equals(DFLT_SNAP_NAME_FOR_RSC))
        {
            // this entry is a SnapshotConnection (TBD), not a Resourceconnection
            ret = null;
        }
        else
        {
            final NodeName nodeNameSrc = raw.build(NODE_NAME_SRC, NodeName::new);
            final NodeName nodeNameDst = raw.build(NODE_NAME_DST, NodeName::new);
            final ResourceName rscName = raw.build(RESOURCE_NAME, ResourceName::new);

            final TcpPortNumber port;
            final long flags;
            switch (getDbType())
            {
                case ETCD:
                    String portStr = raw.get(TCP_PORT);
                    port = portStr != null ? new TcpPortNumber(Integer.parseInt(portStr)) : null;
                    flags = Long.parseLong(raw.get(FLAGS));
                    break;
                case SQL: // fall-through
                case K8S_CRD:
                    port = raw.build(TCP_PORT, TcpPortNumber::new);
                    flags = raw.get(FLAGS);
                    break;
                default:
                    throw new ImplementationError("Unknown database type: " + getDbType());
            }

            ret = new Pair<>(
                ResourceConnection.createForDb(
                    raw.build(UUID, java.util.UUID::fromString),
                    rscMap.get(new Pair<>(nodeNameSrc, rscName)),
                    rscMap.get(new Pair<>(nodeNameDst, rscName)),
                    port,
                    tcpPortPool,
                    this,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    flags
                ),
                null
            );
        }
        return ret;
    }

    @Override
    protected String getId(ResourceConnection rc) throws AccessDeniedException
    {
        Resource sourceRsc = rc.getSourceResource(dbCtx);
        return "(SourceNode=" + sourceRsc.getNode().getName().displayValue +
            " TargetNode=" + rc.getTargetResource(dbCtx).getNode().getName().displayValue +
            " ResName=" + sourceRsc.getResourceDefinition().getName().displayValue + ")";
    }

}
