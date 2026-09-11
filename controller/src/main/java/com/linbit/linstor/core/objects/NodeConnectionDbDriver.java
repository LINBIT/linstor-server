package com.linbit.linstor.core.objects;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeConnections.NODE_NAME_DST;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeConnections.NODE_NAME_SRC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.NodeConnections.UUID;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.Map;

@Singleton
public final class NodeConnectionDbDriver
    extends AbsDatabaseDriver<NodeConnection, Void, Map<NodeName, ? extends Node>>
    implements NodeConnectionCtrlDatabaseDriver
{
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public NodeConnectionDbDriver(
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.NODE_CONNECTIONS, dbEngineRef);
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, nc -> nc.getUuid().toString());
        setColumnSetter(NODE_NAME_SRC, nc -> nc.getSourceNode().getName().value);
        setColumnSetter(NODE_NAME_DST, nc -> nc.getTargetNode().getName().value);
    }

    @Override
    protected Pair<NodeConnection, Void> load(
        RawParameters raw,
        Map<NodeName, ? extends Node> nodesMap
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        return new Pair<>(
            NodeConnection.createForDb(
                raw.build(UUID, java.util.UUID::fromString),
                nodesMap.get(raw.build(NODE_NAME_SRC, NodeName::new)),
                nodesMap.get(raw.build(NODE_NAME_DST, NodeName::new)),
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            ),
            null
        );
    }

    @Override
    protected String getId(NodeConnection nc)
    {
        return "(SourceNode=" + nc.getSourceNode().getName().displayValue +
            " TargetNode=" + nc.getTargetNode().getName().displayValue + ")";
    }

}
