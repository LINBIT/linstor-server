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
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.Triple;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotVolumes.NODE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotVolumes.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotVolumes.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotVolumes.STOR_POOL_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotVolumes.UUID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SnapshotVolumes.VLM_NR;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;

@Singleton
public class SnapshotVolumeDbDriver extends
    AbsDatabaseDriver<SnapshotVolume,
        Void,
        Triple<Map<Triple<NodeName, ResourceName, SnapshotName>, ? extends Snapshot>,
            Map<Triple<ResourceName, SnapshotName, VolumeNumber>, ? extends SnapshotVolumeDefinition>,
            Map<Pair<NodeName, StorPoolName>, ? extends StorPool>>>
    implements SnapshotVolumeDataDatabaseDriver
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
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.SNAPSHOT_VOLUMES, dbEngineRef, objProtDriverRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, snapVlm -> snapVlm.getUuid().toString());
        setColumnSetter(NODE_NAME, snapVlm -> snapVlm.getNodeName().value);
        setColumnSetter(RESOURCE_NAME, snapVlm -> snapVlm.getResourceName().value);
        setColumnSetter(SNAPSHOT_NAME, snapVlm -> snapVlm.getSnapshotName().value);
        setColumnSetter(VLM_NR, snapVlm -> snapVlm.getVolumeNumber().value);
        setColumnSetter(STOR_POOL_NAME, snapVlm -> snapVlm.getStorPool(dbCtxRef).getName().value);
    }

    @Override
    protected Pair<SnapshotVolume, Void> load(
        RawParameters raw,
        Triple<Map<Triple<NodeName, ResourceName, SnapshotName>, ? extends Snapshot>,
            Map<Triple<ResourceName, SnapshotName, VolumeNumber>, ? extends SnapshotVolumeDefinition>,
            Map<Pair<NodeName, StorPoolName>, ? extends StorPool>> loadMaps
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final NodeName nodeName = raw.build(NODE_NAME, NodeName::new);
        final ResourceName rscName = raw.build(RESOURCE_NAME, ResourceName::new);
        final SnapshotName snapName = raw.build(SNAPSHOT_NAME, SnapshotName::new);
        final StorPoolName storPoolName = raw.build(STOR_POOL_NAME, StorPoolName::new);

        final VolumeNumber vlmNr;

        switch (getDbType())
        {
            case ETCD:
                vlmNr = new VolumeNumber(Integer.parseInt(raw.get(VLM_NR)));
                break;
            case SQL:
                vlmNr = raw.build(VLM_NR, VolumeNumber::new);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        return new Pair<>(
            new SnapshotVolumeData(
                raw.build(UUID, java.util.UUID::fromString),
                loadMaps.objA.get(new Triple<>(nodeName, rscName, snapName)),
                loadMaps.objB.get(new Triple<>(rscName, snapName, vlmNr)),
                loadMaps.objC.get(new Pair<>(nodeName, storPoolName)),
                this,
                transObjFactory,
                transMgrProvider
            ),
            null
        );
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
