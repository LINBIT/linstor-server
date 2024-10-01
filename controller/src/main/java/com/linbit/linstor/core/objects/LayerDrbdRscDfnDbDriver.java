package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.ParentObjects;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.SuffixedResourceName;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerDrbdResourceDefinitions;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;
import com.linbit.utils.Triple;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

@Singleton
public class LayerDrbdRscDfnDbDriver
    extends AbsLayerRscDfnDataDbDriver<DrbdRscDfnData<?>, DrbdRscData<?>>
    implements LayerDrbdRscDfnDatabaseDriver
{
    private final DynamicNumberPool tcpPortPool;

    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    private final SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TcpPortNumber> tcpPortDriver;
    private final SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TransportType> transportTypeDriver;
    private final SingleColumnDatabaseDriver<DrbdRscDfnData<?>, String> secretDriver;
    private final SingleColumnDatabaseDriver<DrbdRscDfnData<?>, Short> peerSlotsDriver;

    @Inject
    public LayerDrbdRscDfnDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        super(
            dbCtxRef,
            errorReporterRef,
            GeneratedDatabaseTables.LAYER_DRBD_RESOURCE_DEFINITIONS,
            dbEngineRef
        );
        tcpPortPool = tcpPortPoolRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        setColumnSetter(
            LayerDrbdResourceDefinitions.RESOURCE_NAME,
            drbdRscDfnData -> drbdRscDfnData.getResourceName().value
        );
        setColumnSetter(LayerDrbdResourceDefinitions.RESOURCE_NAME_SUFFIX, DrbdRscDfnData::getRscNameSuffix);
        setColumnSetter(
            LayerDrbdResourceDefinitions.SNAPSHOT_NAME,
            drbdRscDfnData ->
            {
                String ret;
                SnapshotName snapName = drbdRscDfnData.getSnapshotName();
                if (snapName != null)
                {
                    ret = snapName.value;
                }
                else
                {
                    ret = ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;
                }
                return ret;
            }
        );
        /* we are saving on DB level as INTEGER instead of SMALLINT */
        setColumnSetter(
            LayerDrbdResourceDefinitions.PEER_SLOTS,
            drbdRscDfnData -> (int) drbdRscDfnData.getPeerSlots()
        );
        setColumnSetter(LayerDrbdResourceDefinitions.AL_STRIPES, DrbdRscDfnData::getAlStripes);
        setColumnSetter(LayerDrbdResourceDefinitions.AL_STRIPE_SIZE, DrbdRscDfnData::getAlStripeSize);
        setColumnSetter(LayerDrbdResourceDefinitions.TCP_PORT, this::getTcpPort);
        setColumnSetter(
            LayerDrbdResourceDefinitions.TRANSPORT_TYPE,
            drbdRscDfnData -> drbdRscDfnData.getTransportType().name()
        );
        setColumnSetter(LayerDrbdResourceDefinitions.SECRET, DrbdRscDfnData::getSecret);

        tcpPortDriver = generateSingleColumnDriver(
            LayerDrbdResourceDefinitions.TCP_PORT,
            drbdRscDfnData -> "" + getTcpPort(drbdRscDfnData),
            tcpPort -> tcpPort == null ? null : tcpPort.value
        );
        transportTypeDriver = generateSingleColumnDriver(
            LayerDrbdResourceDefinitions.TRANSPORT_TYPE,
            drbdRscDfnData -> drbdRscDfnData.getTransportType().name(),
            TransportType::name
        );
        secretDriver = generateSingleColumnDriver(
            LayerDrbdResourceDefinitions.SECRET,
            DrbdRscDfnData::getSecret,
            Function.identity()
        );
        /* we are saving on DB level as INTEGER instead of SMALLINT */
        peerSlotsDriver = generateSingleColumnDriver(
            LayerDrbdResourceDefinitions.PEER_SLOTS,
            drbdRscDfnData -> "" + drbdRscDfnData.getPeerSlots(),
            Short::intValue
        );
    }

    private @Nullable Integer getTcpPort(DrbdRscDfnData<?> drbdRscDfnData)
    {
        Integer ret = null;
        TcpPortNumber tcpPort = drbdRscDfnData.getTcpPort();
        if (tcpPort != null)
        {
            ret = tcpPort.value;
        }
        return ret;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TcpPortNumber> getTcpPortDriver()
    {
        return tcpPortDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TransportType> getTransportTypeDriver()
    {
        return transportTypeDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, String> getRscDfnSecretDriver()
    {
        return secretDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, Short> getPeerSlotsDriver()
    {
        return peerSlotsDriver;
    }

    @Override
    protected Pair<DrbdRscDfnData<?>, List<DrbdRscData<?>>> load(RawParameters raw, ParentObjects parentRef)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, ExhaustedPoolException, ValueInUseException
    {
        ResourceName rscName = raw.buildParsed(LayerDrbdResourceDefinitions.RESOURCE_NAME, ResourceName::new);
        String rscNameSuffix = raw.getParsed(LayerDrbdResourceDefinitions.RESOURCE_NAME_SUFFIX);
        String snapNameStr = raw.getParsed(LayerDrbdResourceDefinitions.SNAPSHOT_NAME);

        short peerSlots;
        Integer alStripes = raw.getParsed(LayerDrbdResourceDefinitions.AL_STRIPES);
        Long alStripesSize = raw.getParsed(LayerDrbdResourceDefinitions.AL_STRIPE_SIZE);
        Integer port = raw.getParsed(LayerDrbdResourceDefinitions.TCP_PORT);
        TransportType transportType = raw.<String, TransportType, IllegalArgumentException>build(
            LayerDrbdResourceDefinitions.TRANSPORT_TYPE,
            TransportType::byValue
        );
        String secret = raw.getParsed(LayerDrbdResourceDefinitions.SECRET);

        switch (getDbType())
        {
            case SQL:
            case K8S_CRD:
                // bug: by accident we store the peerSlots as INTEGER instead of SMALLINT
                peerSlots = raw.<Integer>get(LayerDrbdResourceDefinitions.PEER_SLOTS).shortValue();
                break;
            case ETCD:
                peerSlots = raw.etcdGetShort(LayerDrbdResourceDefinitions.PEER_SLOTS);
                break;
            default:
                throw new ImplementationError("Unknown db type: " + getDbType());
        }

        SuffixedResourceName suffixedRscName;

        if (snapNameStr == null || snapNameStr.equals(ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC))
        {
            suffixedRscName = new SuffixedResourceName(rscName, null, rscNameSuffix);
            if (port == null)
            {
                throw new DatabaseException("DrbdRscDfnData without tcpPort!");
            }
        }
        else
        {
            suffixedRscName = new SuffixedResourceName(rscName, new SnapshotName(snapNameStr), rscNameSuffix);
            port = DrbdRscDfnData.SNAPSHOT_TCP_PORT;
            secret = null; // just to be sure
        }

        Triple<DrbdRscDfnData<?>, Map<VolumeNumber, DrbdVlmDfnData<?>>, List<DrbdRscData<?>>> triple = genericCreate(
            suffixedRscName,
            peerSlots,
            alStripes,
            alStripesSize,
            port,
            transportType,
            secret
        );

        return new Pair<>(triple.objA, triple.objC);
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>>
        Triple<DrbdRscDfnData<?>,
               Map<VolumeNumber, DrbdVlmDfnData<?>>,
               List<DrbdRscData<?>>> genericCreate(
        SuffixedResourceName suffixedRscNameRef,
        short peerSlotsRef,
        Integer alStripesRef,
        Long alStripesSizeRef,
        Integer portRef,
        TransportType transportTypeRef,
                @Nullable String secretRef
    )
        throws ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException
    {
        TreeMap<VolumeNumber, DrbdVlmDfnData<RSC>> drbdVlmDfnDataMap = new TreeMap<>();
        List<DrbdRscData<RSC>> rscDataList = new ArrayList<>();

        DrbdRscDfnData<RSC> drbdRscDfnData = new DrbdRscDfnData<>(
            suffixedRscNameRef.rscName,
            suffixedRscNameRef.snapName,
            suffixedRscNameRef.rscNameSuffix,
            peerSlotsRef,
            alStripesRef,
            alStripesSizeRef,
            portRef,
            transportTypeRef,
            secretRef,
            rscDataList,
            drbdVlmDfnDataMap,
            tcpPortPool,
            this,
            transObjFactory,
            transMgrProvider
        );
        return new Triple<>(
            drbdRscDfnData,
            (Map<VolumeNumber, DrbdVlmDfnData<?>>) ((Object) drbdVlmDfnDataMap),
            (List<DrbdRscData<?>>) ((Object) rscDataList)
        );
    }
}
