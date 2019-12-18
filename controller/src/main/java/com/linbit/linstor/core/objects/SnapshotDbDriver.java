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
import com.linbit.linstor.core.objects.Snapshot.InitMaps;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
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

import static com.linbit.linstor.core.objects.ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.NODE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.RESOURCE_FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Resources.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

@Singleton
public class SnapshotDbDriver extends
    AbsDatabaseDriver<Snapshot, Snapshot.InitMaps, Pair<Map<NodeName, Node>, Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition>>>
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
        super(errorReporterRef, GeneratedDatabaseTables.RESOURCES, dbEngineRef, objProtDriverRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, snap -> snap.getUuid().toString());
        setColumnSetter(NODE_NAME, snap -> snap.getNodeName().value);
        setColumnSetter(RESOURCE_NAME, snap -> snap.getResourceName().value);
        setColumnSetter(SNAPSHOT_NAME, snap -> snap.getSnapshotName().value);
        setColumnSetter(RESOURCE_FLAGS, snap -> snap.getFlags().getFlagsBits(dbCtxRef));

        flagsDriver = generateFlagDriver(RESOURCE_FLAGS, Snapshot.Flags.class);
    }

    @Override
    public StateFlagsPersistence<Snapshot> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    protected Pair<Snapshot, Snapshot.InitMaps> load(
        RawParameters raw,
        Pair<Map<NodeName, Node>, Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition>> loadMaps
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final Pair<Snapshot, InitMaps> ret;
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

            switch (getDbType())
            {
                case ETCD:
                    flags = Long.parseLong(raw.get(RESOURCE_FLAGS));
                    break;
                case SQL:
                    flags = raw.get(RESOURCE_FLAGS);
                    break;
                default:
                    throw new ImplementationError("Unknown database type: " + getDbType());
            }

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
                    snapshotVlmMap
                ),
                new InitMapsImpl(snapshotVlmMap)
            );
        }
        return ret;
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
