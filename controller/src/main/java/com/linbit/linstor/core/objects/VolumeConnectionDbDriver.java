package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.Triple;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeConnections.NODE_NAME_DST;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeConnections.NODE_NAME_SRC;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeConnections.RESOURCE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeConnections.UUID;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.VolumeConnections.VLM_NR;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;

@Singleton
public class VolumeConnectionDbDriver extends
    AbsDatabaseDriver<VolumeConnectionData, Void, Map<Triple<NodeName, ResourceName, VolumeNumber>, ? extends Volume>>
    implements VolumeConnectionDataDatabaseDriver
{
    private final AccessContext dbCtx;
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public VolumeConnectionDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.VOLUME_CONNECTIONS, dbEngineRef, objProtDriverRef);
        dbCtx = dbCtxRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, vc -> vc.getUuid().toString());
        setColumnSetter(
            NODE_NAME_SRC,
            vc -> vc.getSourceVolume(dbCtxRef).getResource().getAssignedNode().getName().value
        );
        setColumnSetter(
            NODE_NAME_DST,
            vc -> vc.getTargetVolume(dbCtxRef).getResource().getAssignedNode().getName().value
        );
        setColumnSetter(
            RESOURCE_NAME,
            vc -> vc.getSourceVolume(dbCtxRef).getResource().getDefinition().getName().value
        );
        setColumnSetter(VLM_NR, vc -> vc.getSourceVolume(dbCtxRef).getVolumeDefinition().getVolumeNumber().value);
    }

    @Override
    protected Pair<VolumeConnectionData, Void> load(
        RawParameters raw,
        Map<Triple<NodeName, ResourceName, VolumeNumber>, ? extends Volume> vlmsMap
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        final NodeName nodeNameSrc = raw.build(NODE_NAME_SRC, NodeName::new);
        final NodeName nodeNameDst = raw.build(NODE_NAME_DST, NodeName::new);
        final ResourceName rscName = raw.build(RESOURCE_NAME, ResourceName::new);
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
            new VolumeConnectionData(
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

    @Override
    protected String getId(VolumeConnectionData vc) throws AccessDeniedException
    {
        Volume srcVlm = vc.getSourceVolume(dbCtx);
        return "(SourceNode=" + srcVlm.getResource().getAssignedNode().getName().displayValue +
            " TargetNode=" + vc.getTargetVolume(dbCtx).getResource().getAssignedNode().getName().displayValue +
            " ResName=" + srcVlm.getResourceDefinition().getName().displayValue +
            " VolNr=" + srcVlm.getVolumeDefinition().getVolumeNumber().value + ")";
    }

}
