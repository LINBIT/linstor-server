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
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotVolumeDefinitions.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotVolumeDefinitions.SNAPSHOT_FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotVolumeDefinitions.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotVolumeDefinitions.UUID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotVolumeDefinitions.VLM_NR;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotVolumeDefinitions.VLM_SIZE;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Functions;

@Singleton
public class SnapshotVolumeDefinitionDbDriver extends
    AbsDatabaseDriver<SnapshotVolumeDefinition,
        SnapshotVolumeDefinition.InitMaps,
        Map<Pair<ResourceName, SnapshotName>, ? extends SnapshotDefinition>>
    implements SnapshotVolumeDefinitionDatabaseDriver
{
    private final AccessContext dbCtx;
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    private final StateFlagsPersistence<SnapshotVolumeDefinition> flagsDriver;
    private final SingleColumnDatabaseDriver<SnapshotVolumeDefinition, Long> sizeDriver;

    @Inject
    public SnapshotVolumeDefinitionDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.SNAPSHOT_VOLUME_DEFINITIONS, dbEngineRef, objProtDriverRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, snapVlmDfn -> snapVlmDfn.getUuid().toString());
        setColumnSetter(RESOURCE_NAME, snapVlmDfn -> snapVlmDfn.getResourceName().value);
        setColumnSetter(SNAPSHOT_NAME, snapVlmDfn -> snapVlmDfn.getSnapshotName().value);
        setColumnSetter(VLM_NR, snapVlmDfn -> snapVlmDfn.getVolumeNumber().value);
        setColumnSetter(VLM_SIZE, snapVlmDfn -> snapVlmDfn.getVolumeSize(dbCtxRef));
        setColumnSetter(SNAPSHOT_FLAGS, snapVlmDfn -> snapVlmDfn.getFlags().getFlagsBits(dbCtxRef));

        flagsDriver = generateFlagDriver(SNAPSHOT_FLAGS, SnapshotVolumeDefinition.Flags.class);
        sizeDriver = generateSingleColumnDriver(
            VLM_SIZE,
            snapVlmDfn -> Long.toString(snapVlmDfn.getVolumeSize(dbCtxRef)),
            Functions.identity()
        );
    }

    @Override
    public StateFlagsPersistence<SnapshotVolumeDefinition> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<SnapshotVolumeDefinition, Long> getVolumeSizeDriver()
    {
        return sizeDriver;
    }

    @Override
    protected Pair<SnapshotVolumeDefinition, SnapshotVolumeDefinition.InitMaps> load(
        RawParameters raw,
        Map<Pair<ResourceName, SnapshotName>, ? extends SnapshotDefinition> snapDfnMap
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final Map<NodeName, SnapshotVolume> snapshotVlmMap = new TreeMap<>();

        final VolumeNumber vlmNr;
        final long vlmSize;
        final long flags;

        switch (getDbType())
        {
            case ETCD:
                vlmNr = new VolumeNumber(Integer.parseInt(raw.get(VLM_NR)));
                vlmSize = Integer.parseInt(raw.get(VLM_SIZE));
                flags = Long.parseLong(raw.get(SNAPSHOT_FLAGS));
                break;
            case SQL:
                vlmNr = raw.build(VLM_NR, VolumeNumber::new);
                vlmSize = raw.get(VLM_SIZE);
                flags = raw.get(SNAPSHOT_FLAGS);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        return new Pair<>(
            new SnapshotVolumeDefinition(
                raw.build(UUID, java.util.UUID::fromString),
                snapDfnMap.get(
                    new Pair<>(
                        raw.build(RESOURCE_NAME, ResourceName::new),
                        raw.build(SNAPSHOT_NAME, SnapshotName::new)
                    )
                ),
                vlmNr,
                vlmSize,
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

    @Override
    protected String getId(SnapshotVolumeDefinition snapVlmDfn) throws AccessDeniedException
    {
        return "(ResName=" + snapVlmDfn.getResourceName().displayValue +
            " SnapshotName=" + snapVlmDfn.getSnapshotName().displayValue +
            " volumeNr=" + snapVlmDfn.getVolumeNumber().value + ")";
    }

    private class InitMapsImpl implements SnapshotVolumeDefinition.InitMaps
    {
        private final Map<NodeName, SnapshotVolume> snapVlmMap;

        private InitMapsImpl(Map<NodeName, SnapshotVolume> snapVlmMapRef)
        {
            snapVlmMap = snapVlmMapRef;
        }

        @Override
        public Map<NodeName, SnapshotVolume> getSnapshotVlmMap()
        {
            return snapVlmMap;
        }
    }

}
