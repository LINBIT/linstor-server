package com.linbit.linstor.core.objects;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;
import com.linbit.utils.Triple;

import static com.linbit.linstor.core.objects.ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeConnections.NODE_NAME_DST;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeConnections.NODE_NAME_SRC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeConnections.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeConnections.SNAPSHOT_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeConnections.UUID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeConnections.VLM_NR;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.Map;

@Singleton
public final class VolumeConnectionDbDriver
    extends AbsDatabaseDriver<
        VolumeConnection,
        Void,
        Map<Triple<NodeName, ResourceName, VolumeNumber>, Volume>>
    implements VolumeConnectionCtrlDatabaseDriver
{
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public VolumeConnectionDbDriver(
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.VOLUME_CONNECTIONS, dbEngineRef);
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, vc -> vc.getUuid().toString());
        setColumnSetter(
            NODE_NAME_SRC,
            vc -> vc.getSourceVolume().getAbsResource().getNode().getName().value
        );
        setColumnSetter(
            NODE_NAME_DST,
            vc -> vc.getTargetVolume().getAbsResource().getNode().getName().value
        );
        setColumnSetter(
            RESOURCE_NAME,
            vc -> vc.getSourceVolume().getAbsResource().getResourceDefinition().getName().value
        );

        setColumnSetter(VLM_NR, vc -> vc.getSourceVolume().getVolumeDefinition().getVolumeNumber().value);

        setColumnSetter(SNAPSHOT_NAME, ignored -> DFLT_SNAP_NAME_FOR_RSC);
    }

    @Override
    protected @Nullable Pair<VolumeConnection, Void> load(
        RawParameters raw,
        Map<Triple<NodeName, ResourceName, VolumeNumber>, Volume> vlmsMap
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final Pair<VolumeConnection, Void> ret;
        if (!raw.get(SNAPSHOT_NAME).equals(DFLT_SNAP_NAME_FOR_RSC))
        {
            // this entry is a SnapshotVolumeConnection (TBD), not a VolumeConnection
            ret = null;
        }
        else
        {
            final NodeName nodeNameSrc = raw.build(NODE_NAME_SRC, NodeName::new);
            final NodeName nodeNameDst = raw.build(NODE_NAME_DST, NodeName::new);
            final ResourceName rscName = raw.build(RESOURCE_NAME, ResourceName::new);
            final VolumeNumber vlmNr;

            vlmNr = raw.build(VLM_NR, VolumeNumber::new);

            ret = new Pair<>(
                VolumeConnection.createForDb(
                    raw.build(UUID, java.util.UUID::fromString),
                    vlmsMap.get(new Triple<>(nodeNameSrc, rscName, vlmNr)),
                    vlmsMap.get(new Triple<>(nodeNameDst, rscName, vlmNr)),
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
    protected String getId(VolumeConnection vc)
    {
        Volume srcVlm = vc.getSourceVolume();
        return "(SourceNode=" + srcVlm.getAbsResource().getNode().getName().displayValue +
            " TargetNode=" + vc.getTargetVolume().getAbsResource().getNode().getName().displayValue +
            " ResName=" + srcVlm.getResourceDefinition().getName().displayValue +
            " VolNr=" + srcVlm.getVolumeDefinition().getVolumeNumber().value + ")";
    }

}
