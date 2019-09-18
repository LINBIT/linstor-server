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
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotDefinitions.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotDefinitions.SNAPSHOT_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotDefinitions.SNAPSHOT_FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotDefinitions.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotDefinitions.UUID;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

@Singleton
public class SnapshotDefinitionDbDriver
    extends
    AbsDatabaseDriver<SnapshotDefinition,
        SnapshotDefinition.InitMaps,
        Map<ResourceName, ? extends ResourceDefinition>>
    implements SnapshotDefinitionDatabaseDriver
{
    private final AccessContext dbCtx;
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    private final StateFlagsPersistence<SnapshotDefinition> flagsDriver;

    @Inject
    public SnapshotDefinitionDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.SNAPSHOT_DEFINITIONS, dbEngineRef, objProtDriverRef);

        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, snapDfn -> snapDfn.getUuid().toString());
        setColumnSetter(RESOURCE_NAME, snapDfn -> snapDfn.getResourceName().value);
        setColumnSetter(SNAPSHOT_NAME, snapDfn -> snapDfn.getName().value);
        setColumnSetter(SNAPSHOT_DSP_NAME, snapDfn -> snapDfn.getName().displayValue);
        setColumnSetter(SNAPSHOT_FLAGS, snapDfn -> snapDfn.getFlags().getFlagsBits(dbCtxRef));

        flagsDriver = generateFlagDriver(SNAPSHOT_FLAGS, SnapshotDefinition.Flags.class);
    }

    @Override
    public StateFlagsPersistence<SnapshotDefinition> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    protected Pair<SnapshotDefinition, SnapshotDefinition.InitMaps> load(
        RawParameters raw,
        Map<ResourceName, ? extends ResourceDefinition> rscDfnMap
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final Map<VolumeNumber, SnapshotVolumeDefinition> snapshotVlmDfnMap = new TreeMap<>();
        final Map<NodeName, Snapshot> snapshotMap = new TreeMap<>();
        final long flags;

        switch (getDbType())
        {
            case ETCD:
                flags = Long.parseLong(raw.get(SNAPSHOT_FLAGS));
                break;
            case SQL:
                flags = raw.get(SNAPSHOT_FLAGS);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        return new Pair<>(
            new SnapshotDefinition(
                raw.build(UUID, java.util.UUID::fromString),
                rscDfnMap.get(raw.build(RESOURCE_NAME, ResourceName::new)),
                raw.build(SNAPSHOT_DSP_NAME, SnapshotName::new),
                flags,
                this,
                transObjFactory,
                propsContainerFactory,
                transMgrProvider,
                snapshotVlmDfnMap,
                snapshotMap
            ),
            new InitMapsImpl(snapshotMap, snapshotVlmDfnMap)
        );
    }

    @Override
    protected String getId(SnapshotDefinition snapDfn) throws AccessDeniedException
    {
        return "(ResName=" + snapDfn.getResourceName().displayValue +
            " SnapshotName=" + snapDfn.getName().displayValue + ")";
    }

    private class InitMapsImpl implements SnapshotDefinition.InitMaps
    {
        private final Map<NodeName, Snapshot> snapMap;
        private final Map<VolumeNumber, SnapshotVolumeDefinition> snapVlmDfnMap;

        public InitMapsImpl(
            Map<NodeName, Snapshot> snapMapRef,
            Map<VolumeNumber, SnapshotVolumeDefinition> snapVlmDfnMapRef
        )
        {
            snapMap = snapMapRef;
            snapVlmDfnMap = snapVlmDfnMapRef;
        }

        @Override
        public Map<NodeName, Snapshot> getSnapshotMap()
        {
            return snapMap;
        }

        @Override
        public Map<VolumeNumber, SnapshotVolumeDefinition> getSnapshotVolumeDefinitionMap()
        {
            return snapVlmDfnMap;
        }

    }
}
