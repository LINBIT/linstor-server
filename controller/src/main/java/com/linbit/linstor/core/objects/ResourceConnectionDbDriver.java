package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.NODE_NAME_DST;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.NODE_NAME_SRC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.TCP_PORT;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.UUID;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Objects;

@Singleton
public class ResourceConnectionDbDriver
    extends AbsDatabaseDriver<ResourceConnectionData, Void, Map<Pair<NodeName, ResourceName>, ? extends Resource>>
    implements ResourceConnectionDataDatabaseDriver
{
    private final AccessContext dbCtx;
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final DynamicNumberPool tcpPortPool;

    private final StateFlagsPersistence<ResourceConnectionData> flagsDriver;
    private final SingleColumnDatabaseDriver<ResourceConnectionData, TcpPortNumber> portDriver;

    @Inject
    public ResourceConnectionDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.RESOURCE_CONNECTIONS, dbEngineRef, objProtDriverRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        tcpPortPool = tcpPortPoolRef;

        setColumnSetter(UUID, rc -> rc.getUuid().toString());
        setColumnSetter(NODE_NAME_SRC, rc -> rc.getSourceResource(dbCtxRef).getAssignedNode().getName().value);
        setColumnSetter(NODE_NAME_DST, rc -> rc.getTargetResource(dbCtxRef).getAssignedNode().getName().value);
        setColumnSetter(RESOURCE_NAME, rc -> rc.getSourceResource(dbCtxRef).getDefinition().getName().value);
        setColumnSetter(FLAGS, rc -> rc.getStateFlags().getFlagsBits(dbCtxRef));
        setColumnSetter(TCP_PORT, rc -> TcpPortNumber.getValueNullable(rc.getPort(dbCtxRef)));

        flagsDriver = generateFlagDriver(FLAGS, ResourceConnection.RscConnFlags.class);
        portDriver = generateSingleColumnDriver(
            TCP_PORT,
            rc -> Objects.toString(rc.getPort(dbCtxRef)),
            TcpPortNumber::getValueNullable
        );
    }

    @Override
    public StateFlagsPersistence<ResourceConnectionData> getStateFlagPersistence()
    {
        return flagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceConnectionData, TcpPortNumber> getPortDriver()
    {
        return portDriver;
    }

    @Override
    protected Pair<ResourceConnectionData, Void> load(
        RawParameters raw,
        Map<Pair<NodeName, ResourceName>, ? extends Resource> rscMap
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
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
            case SQL:
                port = raw.build(TCP_PORT, TcpPortNumber::new);
                flags = raw.get(FLAGS);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        return new Pair<>(
            new ResourceConnectionData(
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

    @Override
    protected String getId(ResourceConnectionData rc) throws AccessDeniedException
    {
        Resource sourceRsc = rc.getSourceResource(dbCtx);
        return "(SourceNode=" + sourceRsc.getAssignedNode().getName().displayValue +
            " TargetNode=" + rc.getTargetResource(dbCtx).getAssignedNode().getName().displayValue +
            " ResName=" + sourceRsc.getDefinition().getName().displayValue + ")";
    }

}
