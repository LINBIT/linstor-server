package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Snapshots.LAYER_STACK;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Snapshots.NODE_ID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Snapshots.NODE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Snapshots.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Snapshots.SNAPSHOT_FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Snapshots.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Snapshots.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

@Singleton
public class SnapshotDbDriver extends
    AbsDatabaseDriver<Snapshot,
        Snapshot.InitMaps,
        Pair<Map<NodeName, ? extends Node>,
            Map<Pair<ResourceName, SnapshotName>, ? extends SnapshotDefinition>>>
    implements SnapshotCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    private final StateFlagsPersistence<Snapshot> flagsDriver;

    @Inject
    public SnapshotDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.SNAPSHOTS, dbEngineRef, objProtDriverRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, snap -> snap.getUuid().toString());
        setColumnSetter(NODE_NAME, snap -> snap.getNodeName().value);
        setColumnSetter(RESOURCE_NAME, snap -> snap.getResourceName().value);
        setColumnSetter(SNAPSHOT_NAME, snap -> snap.getSnapshotName().value);
        setColumnSetter(SNAPSHOT_FLAGS, snap -> snap.getFlags().getFlagsBits(dbCtxRef));
        setColumnSetter(NODE_ID, snap -> snap.getNodeId() == null ? null : snap.getNodeId().value);
        setColumnSetter(LAYER_STACK, snap -> toString(StringUtils.asStrList(snap.getLayerStack(dbCtxRef))));

        flagsDriver = generateFlagDriver(SNAPSHOT_FLAGS, Snapshot.Flags.class);
    }

    @Override
    public StateFlagsPersistence<Snapshot> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    protected Pair<Snapshot, Snapshot.InitMaps> load(
        RawParameters raw,
        Pair<Map<NodeName, ? extends Node>,
            Map<Pair<ResourceName, SnapshotName>, ? extends SnapshotDefinition>> loadMaps
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final Map<VolumeNumber, SnapshotVolume> snapshotVlmMap = new TreeMap<>();

        final NodeId nodeId;
        final long flags;

        switch (getDbType())
        {
            case ETCD:
                nodeId = new NodeId(Integer.parseInt(raw.get(NODE_ID)));
                flags = Long.parseLong(raw.get(SNAPSHOT_FLAGS));
                break;
            case SQL:
                nodeId = raw.build(NODE_ID, NodeId::new);
                flags = raw.get(SNAPSHOT_FLAGS);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }
        return new Pair<>(
            new Snapshot(
                raw.build(UUID, java.util.UUID::fromString),
                loadMaps.objB.get(
                    new Pair<>(
                        raw.build(RESOURCE_NAME, ResourceName::new),
                        raw.build(SNAPSHOT_NAME, SnapshotName::new)
                    )
                ),
                loadMaps.objA.get(raw.build(NODE_NAME, NodeName::new)),
                nodeId,
                flags,
                this,
                transObjFactory,
                transMgrProvider,
                snapshotVlmMap,
                DatabaseLoader.asDevLayerKindList(raw.getAsStringList(LAYER_STACK))
            ),
            new InitMapsImpl(snapshotVlmMap)
        );
    }

    @Override
    protected String getId(Snapshot snap) throws AccessDeniedException
    {
        return "(NodeName=" + snap.getNodeName().displayValue +
            " ResName=" + snap.getResourceName().displayValue +
            " SnapshotName=" + snap.getSnapshotName().displayValue + ")";
    }

    private class InitMapsImpl implements Snapshot.InitMaps
    {
        private final Map<VolumeNumber, SnapshotVolume> snapVlmMap;

        InitMapsImpl(Map<VolumeNumber, SnapshotVolume> snapVlmMapRef)
        {
            snapVlmMap = snapVlmMapRef;
        }

        @Override
        public Map<VolumeNumber, SnapshotVolume> getSnapshotVlmMap()
        {
            return snapVlmMap;
        }
    }
}
