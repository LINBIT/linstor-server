package com.linbit.linstor.core.objects;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.SnapshotDefinition.InitMaps;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.core.objects.ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.LAYER_STACK;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.PARENT_UUID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_EXTERNAL_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_GROUP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.SNAPSHOT_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.ResourceDefinitions.UUID;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public final class SnapshotDefinitionDbDriver
    extends AbsDatabaseDriver<SnapshotDefinition,
        SnapshotDefinition.InitMaps,
        Map<ResourceName, ResourceDefinition>>
    implements SnapshotDefinitionCtrlDatabaseDriver
{
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    private final StateFlagsPersistence<SnapshotDefinition> flagsDriver;

    @Inject
    public SnapshotDefinitionDbDriver(
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.RESOURCE_DEFINITIONS, dbEngineRef);

        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, snapDfn -> snapDfn.getUuid().toString());
        setColumnSetter(RESOURCE_NAME, snapDfn -> snapDfn.getResourceName().value);
        setColumnSetter(SNAPSHOT_NAME, snapDfn -> snapDfn.getName().value);
        setColumnSetter(SNAPSHOT_DSP_NAME, snapDfn -> snapDfn.getName().displayValue);
        setColumnSetter(RESOURCE_FLAGS, snapDfn -> snapDfn.getFlags().getFlagsBits());
        setColumnSetter(
            RESOURCE_GROUP_NAME, snapDfn -> snapDfn.getResourceDefinition().getResourceGroup().getName().value
        );
        setColumnSetter(PARENT_UUID, snapDfn -> snapDfn.getResourceDefinition().getUuid().toString());

        setColumnSetter(RESOURCE_EXTERNAL_NAME, ignored -> null);
        setColumnSetter(RESOURCE_DSP_NAME, ignored -> null);
        setColumnSetter(LAYER_STACK, ignored -> toString(Collections.emptyList()));

        flagsDriver = generateFlagDriver(RESOURCE_FLAGS, SnapshotDefinition.Flags.class);
    }

    @Override
    public StateFlagsPersistence<SnapshotDefinition> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    protected @Nullable Pair<SnapshotDefinition, SnapshotDefinition.InitMaps> load(
        RawParameters raw,
        Map<ResourceName, ResourceDefinition> rscDfnMap
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final Pair<SnapshotDefinition, InitMaps> ret;
        final String snapNameStr = raw.get(SNAPSHOT_DSP_NAME);
        if (snapNameStr.equals(DFLT_SNAP_NAME_FOR_RSC))
        {
            // this entry is a ResourceDefinition, not a SnapshotDefinition
            ret = null;
        }
        else
        {
            final Map<VolumeNumber, SnapshotVolumeDefinition> snapshotVlmDfnMap = new TreeMap<>();
            final Map<NodeName, Snapshot> snapshotMap = new TreeMap<>();
            final long flags;

            final ResourceName rscName = raw.build(RESOURCE_NAME, ResourceName::new);
            final SnapshotName snapName = raw.<String, SnapshotName, InvalidNameException>build(
                SNAPSHOT_DSP_NAME,
                SnapshotName::new
            );

            flags = raw.get(RESOURCE_FLAGS);

            ret = new Pair<>(
                new SnapshotDefinition(
                    raw.build(UUID, java.util.UUID::fromString),
                    rscDfnMap.get(rscName),
                    snapName,
                    flags,
                    this,
                    transObjFactory,
                    propsContainerFactory,
                    transMgrProvider,
                    snapshotVlmDfnMap,
                    snapshotMap,
                    new TreeMap<>()
                ),
                new InitMapsImpl(snapshotMap, snapshotVlmDfnMap)
            );
        }
        return ret;
    }

    @Override
    protected String getId(SnapshotDefinition snapDfn)
    {
        return "(ResName=" + snapDfn.getResourceName().displayValue +
            " SnapshotName=" + snapDfn.getName().displayValue + ")";
    }

    private static class InitMapsImpl implements SnapshotDefinition.InitMaps
    {
        private final Map<NodeName, Snapshot> snapMap;
        private final Map<VolumeNumber, SnapshotVolumeDefinition> snapVlmDfnMap;

        InitMapsImpl(
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
