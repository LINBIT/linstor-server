package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Volume.InitMaps;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.VolumeCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.core.objects.ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Volumes.NODE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Volumes.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Volumes.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Volumes.UUID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Volumes.VLM_FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Volumes.VLM_NR;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

@Singleton
public class VolumeDbDriver
    extends AbsDatabaseDriver<Volume,
        Volume.InitMaps,
        Pair<Map<Pair<NodeName, ResourceName>, ? extends Resource>,
            Map<Pair<ResourceName, VolumeNumber>, ? extends VolumeDefinition>>>
    implements VolumeCtrlDatabaseDriver
{

    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    private final StateFlagsPersistence<Volume> flagsDriver;

    @Inject
    public VolumeDbDriver(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext dbCtxRef,
        DbEngine dbEngineRef,
        ObjectProtectionFactory objProtFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.VOLUMES, dbEngineRef, objProtFactoryRef);
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, vlm -> vlm.getUuid().toString());
        setColumnSetter(NODE_NAME, vlm -> vlm.getAbsResource().getNode().getName().value);
        setColumnSetter(RESOURCE_NAME, vlm -> vlm.getResourceDefinition().getName().value);
        setColumnSetter(VLM_NR, vlm -> vlm.getVolumeDefinition().getVolumeNumber().value);
        setColumnSetter(VLM_FLAGS, vlm -> vlm.getFlags().getFlagsBits(dbCtxRef));

        setColumnSetter(SNAPSHOT_NAME, ignored -> DFLT_SNAP_NAME_FOR_RSC);

        flagsDriver = generateFlagDriver(VLM_FLAGS, Volume.Flags.class);

    }

    @Override
    public StateFlagsPersistence<Volume> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    protected @Nullable Pair<Volume, Volume.InitMaps> load(
        RawParameters raw,
        Pair<Map<Pair<NodeName, ResourceName>, ? extends Resource>,
            Map<Pair<ResourceName, VolumeNumber>, ? extends VolumeDefinition>> parentRef
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException
    {
        final Pair<Volume, InitMaps> ret;
        if (!raw.get(SNAPSHOT_NAME).equals(DFLT_SNAP_NAME_FOR_RSC))
        {
            // this entry is a SnapshotVolume, not a Volume
            ret = null;
        }
        else
        {
            final NodeName nodeName = raw.build(NODE_NAME, NodeName::new);
            final ResourceName rscName = raw.build(RESOURCE_NAME, ResourceName::new);
            final VolumeNumber vlmNr;

            final long flags;
            switch (getDbType())
            {
                case ETCD:
                    flags = Long.parseLong(raw.get(VLM_FLAGS));
                    vlmNr = new VolumeNumber(Integer.parseInt(raw.get(VLM_NR)));
                    break;
                case SQL: // fall-through
                case K8S_CRD:
                    flags = raw.get(VLM_FLAGS);
                    vlmNr = raw.build(VLM_NR, VolumeNumber::new);
                    break;
                default:
                    throw new ImplementationError("Unknown database type: " + getDbType());
            }

            final Map<Volume.Key, VolumeConnection> vlmConnsMap = new TreeMap<>();
            ret = new Pair<>(
                new Volume(
                    raw.build(UUID, java.util.UUID::fromString),
                    parentRef.objA.get(new Pair<>(nodeName, rscName)),
                    parentRef.objB.get(new Pair<>(rscName, vlmNr)),
                    flags,
                    this,
                    vlmConnsMap,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                ),
                new VolumeInitMaps(
                    vlmConnsMap
                )
            );
        }
        return ret;
    }

    @Override
    protected String getId(Volume vlm)
    {
        return "(NodeName=" + vlm.getAbsResource().getNode().getName().displayValue +
            " ResName=" + vlm.getResourceDefinition().getName().displayValue +
            " VolNum=" + vlm.getVolumeDefinition().getVolumeNumber().value + ")";
    }

    private class VolumeInitMaps implements Volume.InitMaps
    {
        private final Map<Volume.Key, VolumeConnection> vlmConnMap;

        VolumeInitMaps(Map<Volume.Key, VolumeConnection> vlmConnMapRef)
        {
            vlmConnMap = vlmConnMapRef;
        }

        @Override
        public Map<Volume.Key, VolumeConnection> getVolumeConnections()
        {
            return vlmConnMap;
        }
    }
}
