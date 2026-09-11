package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Snapshot.InitMaps;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import static com.linbit.linstor.core.objects.ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.CREATE_TIMESTAMP;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.NODE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.RESOURCE_FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.UUID;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

@Singleton
public final class SnapshotDbDriver extends
    AbsDatabaseDriver<AbsResource<Snapshot>, Snapshot.InitMaps, PairNonNull<Map<NodeName, Node>,
    Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition>>>
    implements SnapshotCtrlDatabaseDriver
{
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    private final StateFlagsPersistence<AbsResource<Snapshot>> flagsDriver;
    private final SingleColumnDatabaseDriver<AbsResource<Snapshot>, Instant> createTimestampDriver;

    @Inject
    public SnapshotDbDriver(
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.RESOURCES, dbEngineRef);
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        Function<Instant, Object> createTimestampTypeMapper;

        setColumnSetter(UUID, snap -> snap.getUuid().toString());
        setColumnSetter(NODE_NAME, snap -> snap.getNode().getName().value);
        setColumnSetter(RESOURCE_NAME, snap -> snap.getResourceDefinition().getName().value);
        setColumnSetter(SNAPSHOT_NAME, snap -> ((Snapshot) snap).getSnapshotName().value);
        setColumnSetter(RESOURCE_FLAGS, snap -> ((Snapshot) snap).getFlags().getFlagsBits());
        switch (getDbType())
        {
            case SQL ->
            {
                setColumnSetter(
                    CREATE_TIMESTAMP,
                    snap -> snap.getCreateTimestamp().isPresent() ?
                        new Timestamp(snap.getCreateTimestamp().get().toEpochMilli()) :
                        null
                );
                createTimestampTypeMapper = createTime -> createTime != null ?
                    new Timestamp(createTime.toEpochMilli()) :
                    null;
            }
            case K8S_CRD ->
            {
                setColumnSetter(
                    CREATE_TIMESTAMP,
                    snap -> snap.getCreateTimestamp().isPresent() ?
                        snap.getCreateTimestamp().get().toEpochMilli() :
                        null
                );
                createTimestampTypeMapper = createTime -> createTime != null ? createTime.toEpochMilli() : null;
            }
            default -> throw new ImplementationError("Unknown database type: " + getDbType());
        }

        flagsDriver = generateFlagDriver(RESOURCE_FLAGS, Snapshot.Flags.class);

        createTimestampDriver = generateSingleColumnDriver(
            CREATE_TIMESTAMP,
            snap -> snap.getCreateTimestamp().isPresent() ? snap.getCreateTimestamp().get().toString() : "null",
            createTimestampTypeMapper
        );
    }

    @Override
    public StateFlagsPersistence<AbsResource<Snapshot>> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    protected @Nullable Pair<AbsResource<Snapshot>, Snapshot.InitMaps> load(
        RawParameters raw,
        PairNonNull<Map<NodeName, Node>, Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition>> loadMaps
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final Pair<AbsResource<Snapshot>, InitMaps> ret;
        final String snapNameStr = raw.get(SNAPSHOT_NAME);
        if (snapNameStr.equals(DFLT_SNAP_NAME_FOR_RSC))
        {
            // this entry is a Resource, not a Snapshot
            ret = null;
        }
        else
        {
            final Map<VolumeNumber, SnapshotVolume> snapshotVlmMap = new TreeMap<>();

            final long flags;

            flags = raw.get(RESOURCE_FLAGS);
            Long createTimestamp = raw.get(CREATE_TIMESTAMP);

            ret = new Pair<>(
                new Snapshot(
                    raw.build(UUID, java.util.UUID::fromString),
                    loadMaps.objB.get(
                        new Pair<>(
                            raw.build(RESOURCE_NAME, ResourceName::new),
                            new SnapshotName(snapNameStr)
                        )
                    ),
                    loadMaps.objA.get(raw.build(NODE_NAME, NodeName::new)),
                    flags,
                    this,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    snapshotVlmMap,
                    createTimestamp == null ? null : Instant.ofEpochMilli(createTimestamp)
                ),
                new InitMapsImpl(snapshotVlmMap)
            );
        }
        return ret;
    }

    @Override
    protected String getId(AbsResource<Snapshot> absSnap)
    {
        Snapshot snap = (Snapshot) absSnap;
        return "(NodeName=" + snap.getNodeName().displayValue +
            " ResName=" + snap.getResourceName().displayValue +
            " SnapshotName=" + snap.getSnapshotName().displayValue + ")";
    }

    private static class InitMapsImpl implements Snapshot.InitMaps
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

    @Override
    public SingleColumnDatabaseDriver<AbsResource<Snapshot>, Instant> getCreateTimeDriver()
    {
        return createTimestampDriver;
    }
}
