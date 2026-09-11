package com.linbit.linstor.core.objects;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
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
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.TCP_PORT_DST;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.TCP_PORT_SRC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceConnections.UUID;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.Map;
import java.util.Objects;

@Singleton
public final class ResourceConnectionDbDriver
    extends AbsDatabaseDriver<ResourceConnection, Void, Map<Pair<NodeName, ResourceName>, ? extends Resource>>
    implements ResourceConnectionCtrlDatabaseDriver
{
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    private final StateFlagsPersistence<ResourceConnection> flagsDriver;
    private final SingleColumnDatabaseDriver<ResourceConnection, TcpPortNumber> drbdProxyPortSourceDriver;
    private final SingleColumnDatabaseDriver<ResourceConnection, TcpPortNumber> drbdProxyPortTargetDriver;

    @Inject
    public ResourceConnectionDbDriver(
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.RESOURCE_CONNECTIONS, dbEngineRef);
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, rc -> rc.getUuid().toString());
        setColumnSetter(NODE_NAME_SRC, rc -> rc.getSourceResource().getNode().getName().value);
        setColumnSetter(NODE_NAME_DST, rc -> rc.getTargetResource().getNode().getName().value);
        setColumnSetter(RESOURCE_NAME, rc -> rc.getSourceResource().getResourceDefinition().getName().value);
        setColumnSetter(FLAGS, rc -> rc.getStateFlags().getFlagsBits());
        setColumnSetter(TCP_PORT_SRC, rc -> TcpPortNumber.getValueNullable(rc.getDrbdProxyPortSource()));
        setColumnSetter(TCP_PORT_DST, rc -> TcpPortNumber.getValueNullable(rc.getDrbdProxyPortTarget()));

        setColumnSetter(SNAPSHOT_NAME, ignored -> DFLT_SNAP_NAME_FOR_RSC);

        flagsDriver = generateFlagDriver(FLAGS, ResourceConnection.Flags.class);
        drbdProxyPortSourceDriver = generateSingleColumnDriver(
            TCP_PORT_SRC,
            rc -> Objects.toString(rc.getDrbdProxyPortSource()),
            TcpPortNumber::getValueNullable
        );
        drbdProxyPortTargetDriver = generateSingleColumnDriver(
            TCP_PORT_DST,
            rc -> Objects.toString(rc.getDrbdProxyPortTarget()),
            TcpPortNumber::getValueNullable
        );
    }

    @Override
    public StateFlagsPersistence<ResourceConnection> getStateFlagPersistence()
    {
        return flagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceConnection, TcpPortNumber> getDrbdProxyPortSourceDriver()
    {
        return drbdProxyPortSourceDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceConnection, TcpPortNumber> getDrbdProxyPortTargetDriver()
    {
        return drbdProxyPortTargetDriver;
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

            final @Nullable TcpPortNumber portSrc;
            final @Nullable TcpPortNumber portDst;
            final long flags;
            portSrc = raw.build(TCP_PORT_SRC, TcpPortNumber::new);
            portDst = raw.build(TCP_PORT_DST, TcpPortNumber::new);

            flags = raw.get(FLAGS);

            ret = new Pair<>(
                ResourceConnection.createForDb(
                    raw.build(UUID, java.util.UUID::fromString),
                    rscMap.get(new Pair<>(nodeNameSrc, rscName)),
                    rscMap.get(new Pair<>(nodeNameDst, rscName)),
                    portSrc,
                    portDst,
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
    protected String getId(ResourceConnection rc)
    {
        Resource sourceRsc = rc.getSourceResource();
        return "(SourceNode=" + sourceRsc.getNode().getName().displayValue +
            " TargetNode=" + rc.getTargetResource().getNode().getName().displayValue +
            " ResName=" + sourceRsc.getResourceDefinition().getName().displayValue + ")";
    }

}
