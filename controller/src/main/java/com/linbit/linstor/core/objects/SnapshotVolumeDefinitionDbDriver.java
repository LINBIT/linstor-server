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
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition.InitMaps;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeDefinitions.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeDefinitions.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeDefinitions.UUID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeDefinitions.VLM_FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeDefinitions.VLM_NR;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeDefinitions.VLM_SIZE;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

@Singleton
public class SnapshotVolumeDefinitionDbDriver extends
    AbsDatabaseDriver<SnapshotVolumeDefinition,
        SnapshotVolumeDefinition.InitMaps,
        Pair<
            Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition>,
            Map<Pair<ResourceName, VolumeNumber>, VolumeDefinition>>>
    implements SnapshotVolumeDefinitionCtrlDatabaseDriver
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
        ObjectProtectionFactory objProtFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.VOLUME_DEFINITIONS, dbEngineRef, objProtFactoryRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, snapVlmDfn -> snapVlmDfn.getUuid().toString());
        setColumnSetter(RESOURCE_NAME, snapVlmDfn -> snapVlmDfn.getResourceName().value);
        setColumnSetter(SNAPSHOT_NAME, snapVlmDfn -> snapVlmDfn.getSnapshotName().value);
        setColumnSetter(VLM_NR, snapVlmDfn -> snapVlmDfn.getVolumeNumber().value);
        setColumnSetter(VLM_SIZE, snapVlmDfn -> snapVlmDfn.getVolumeSize(dbCtxRef));
        setColumnSetter(VLM_FLAGS, snapVlmDfn -> snapVlmDfn.getFlags().getFlagsBits(dbCtxRef));

        flagsDriver = generateFlagDriver(VLM_FLAGS, SnapshotVolumeDefinition.Flags.class);
        sizeDriver = generateSingleColumnDriver(
            VLM_SIZE,
            snapVlmDfn -> Long.toString(snapVlmDfn.getVolumeSize(dbCtxRef)),
            Function.identity()
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
    protected @Nullable Pair<SnapshotVolumeDefinition, SnapshotVolumeDefinition.InitMaps> load(
        RawParameters raw,
        Pair<Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition>,
            Map<Pair<ResourceName, VolumeNumber>, VolumeDefinition>> parentObjs
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final Pair<SnapshotVolumeDefinition, InitMaps> ret;
        final String snapNameStr = raw.get(SNAPSHOT_NAME);
        if (snapNameStr.equals(ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC))
        {
            // this entry is a VolumeDefinition, not a SnapshotVolumeDefinition
            ret = null;
        }
        else
        {
            final ResourceName rscName = raw.build(RESOURCE_NAME, ResourceName::new);
            final Map<NodeName, SnapshotVolume> snapshotVlmMap = new TreeMap<>();

            final VolumeNumber vlmNr;
            final long vlmSize;
            final long flags;

            switch (getDbType())
            {
                case ETCD:
                    vlmNr = new VolumeNumber(Integer.parseInt(raw.get(VLM_NR)));
                    vlmSize = Integer.parseInt(raw.get(VLM_SIZE));
                    flags = Long.parseLong(raw.get(VLM_FLAGS));
                    break;
                case SQL: // fall-through
                case K8S_CRD:
                    vlmNr = raw.build(VLM_NR, VolumeNumber::new);
                    vlmSize = raw.get(VLM_SIZE);
                    flags = raw.get(VLM_FLAGS);
                    break;
                default:
                    throw new ImplementationError("Unknown database type: " + getDbType());
            }

            ret = new Pair<>(
                new SnapshotVolumeDefinition(
                    raw.build(UUID, java.util.UUID::fromString),
                    parentObjs.objA.get(
                        new Pair<>(
                            rscName,
                            new SnapshotName(snapNameStr)
                        )
                    ),
                    parentObjs.objB.get(new Pair<>(rscName, vlmNr)),
                    vlmNr,
                    vlmSize,
                    flags,
                    this,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    snapshotVlmMap,
                    new TreeMap<>()
                ),
                new InitMapsImpl(snapshotVlmMap)
            );
        }
        return ret;
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
