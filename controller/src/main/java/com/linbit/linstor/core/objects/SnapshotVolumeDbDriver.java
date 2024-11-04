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
import com.linbit.linstor.dbdrivers.AbsProtectedDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;
import com.linbit.utils.Triple;

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

@Singleton
public class SnapshotVolumeDbDriver
    extends AbsProtectedDatabaseDriver<
        SnapshotVolume,
        Void,
        PairNonNull<
            Map<Triple<NodeName, ResourceName, SnapshotName>, ? extends Snapshot>,
            Map<Triple<ResourceName, SnapshotName, VolumeNumber>, ? extends SnapshotVolumeDefinition>>>
    implements SnapshotVolumeCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public SnapshotVolumeDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionFactory objProtFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.VOLUMES, dbEngineRef, objProtFactoryRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, snapVlm -> snapVlm.getUuid().toString());
        setColumnSetter(NODE_NAME, snapVlm -> snapVlm.getNodeName().value);
        setColumnSetter(RESOURCE_NAME, snapVlm -> snapVlm.getResourceName().value);
        setColumnSetter(SNAPSHOT_NAME, snapVlm -> snapVlm.getSnapshotName().value);
        setColumnSetter(VLM_NR, snapVlm -> snapVlm.getVolumeNumber().value);
        setColumnSetter(VLM_FLAGS, ignored -> 0L);
    }

    @Override
    protected @Nullable Pair<SnapshotVolume, Void> load(
        RawParameters raw,
        PairNonNull<Map<Triple<NodeName, ResourceName, SnapshotName>, ? extends Snapshot>, Map<Triple<ResourceName, SnapshotName, VolumeNumber>, ? extends SnapshotVolumeDefinition>> loadMaps
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final Pair<SnapshotVolume, Void> ret;
        final String snapNameStr = raw.get(SNAPSHOT_NAME);
        if (snapNameStr.equals(DFLT_SNAP_NAME_FOR_RSC))
        {
            // this entry is a Volume, not a SnapshotVolume
            ret = null;
        }
        else
        {
            final NodeName nodeName = raw.build(NODE_NAME, NodeName::new);
            final ResourceName rscName = raw.build(RESOURCE_NAME, ResourceName::new);
            final SnapshotName snapName = new SnapshotName(snapNameStr);

            final VolumeNumber vlmNr;

            switch (getDbType())
            {
                case ETCD:
                    vlmNr = new VolumeNumber(Integer.parseInt(raw.get(VLM_NR)));
                    break;
                case SQL: // fall-through
                case K8S_CRD:
                    vlmNr = raw.build(VLM_NR, VolumeNumber::new);
                    break;
                default:
                    throw new ImplementationError("Unknown database type: " + getDbType());
            }

            ret = new Pair<>(
                new SnapshotVolume(
                    raw.build(UUID, java.util.UUID::fromString),
                    loadMaps.objA.get(new Triple<>(nodeName, rscName, snapName)),
                    loadMaps.objB.get(new Triple<>(rscName, snapName, vlmNr)),
                    this,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                ),
                null
            );
        }
        return ret;
    }

    @Override
    protected String getId(SnapshotVolume snapVlm) throws AccessDeniedException
    {
        return "(NodeName=" + snapVlm.getNodeName().displayValue +
            " ResName=" + snapVlm.getResourceName().displayValue +
            " SnapshotName=" + snapVlm.getSnapshotName().displayValue +
            " VolumeNr=" + snapVlm.getVolumeNumber().value + ")";
    }

}
