package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerDrbdResources;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.PairNonNull;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Singleton
public class LayerDrbdRscDbDriver
    extends AbsLayerRscDataDbDriver<DrbdRscDfnData<?>, DrbdVlmDfnData<?>, DrbdRscData<?>, DrbdVlmData<?>>
    implements LayerDrbdRscCtrlDatabaseDriver
{
    private final SingleColumnDatabaseDriver<DrbdRscData<?>, NodeId> nodeIdDriver;
    private final StateFlagsPersistence<DrbdRscData<?>> flagsDriver;
    private final LayerDrbdVlmDbDriver drbdVlmDriver;

    @Inject
    public LayerDrbdRscDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        LayerResourceIdDatabaseDriver rscLayerIdDriverRef,
        LayerDrbdRscDfnDbDriver drbdRscDfnDriverRef,
        LayerDrbdVlmDfnDbDriver drbdVlmDfnDriverRef,
        LayerDrbdVlmDbDriver drbdVlmDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        super(
            dbCtxRef,
            errorReporterRef,
            GeneratedDatabaseTables.LAYER_DRBD_RESOURCES,
            LayerDrbdResources.LAYER_RESOURCE_ID,
            dbEngineRef,
            rscLayerIdDriverRef,
            drbdRscDfnDriverRef,
            drbdVlmDfnDriverRef,
            drbdVlmDriverRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
        drbdVlmDriver = drbdVlmDriverRef;

        setColumnSetter(LayerDrbdResources.LAYER_RESOURCE_ID, DrbdRscData::getRscLayerId);
        /* we are saving on DB level as INTEGER instead of SMALLINT */
        setColumnSetter(LayerDrbdResources.PEER_SLOTS, drbdRscData -> new Short(drbdRscData.getPeerSlots()).intValue());
        setColumnSetter(LayerDrbdResources.AL_STRIPES, DrbdRscData::getAlStripes);
        setColumnSetter(LayerDrbdResources.AL_STRIPE_SIZE, DrbdRscData::getAlStripeSize);
        setColumnSetter(LayerDrbdResources.FLAGS, drbdData -> drbdData.getFlags().getFlagsBits(dbCtxRef));
        setColumnSetter(LayerDrbdResources.NODE_ID, drbdData -> drbdData.getNodeId().value);

        nodeIdDriver = generateSingleColumnDriver(
            LayerDrbdResources.NODE_ID,
            drbdRscData -> Integer.toString(drbdRscData.getNodeId().value),
            nodeId -> nodeId.value
        );
        flagsDriver = generateFlagDriver(LayerDrbdResources.FLAGS, DrbdRscObject.DrbdRscFlags.class);
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscData<?>, NodeId> getNodeIdDriver()
    {
        return nodeIdDriver;
    }

    @Override
    public StateFlagsPersistence<DrbdRscData<?>> getRscStateFlagPersistence()
    {
        return flagsDriver;
    }

    @Override
    public DeviceLayerKind getDeviceLayerKind()
    {
        return DeviceLayerKind.DRBD;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <RSC extends AbsResource<RSC>> RscDataLoadOutput<DrbdRscData<?>, DrbdVlmData<?>> loadImpl(
        RawParameters rawRef,
        ParentObjects parentRef,
        AbsRscLayerObject<?> currentDummyLoadingRLORef,
        PairNonNull<NodeName, SuffixedResourceName> nodeNameSuffixedRscNamePairRef,
        AbsRscLayerObject<?> loadedParentRscDataRef,
        RSC absRscRef
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        Set<AbsRscLayerObject<?>> typeErasedChildren;
        Map<VolumeNumber, DrbdVlmData<?>> typeErasedVlmMap;

        int lri = rawRef.getParsed(LayerDrbdResources.LAYER_RESOURCE_ID);
        NodeId nodeId = rawRef.buildParsed(LayerDrbdResources.NODE_ID, NodeId::new);
        short peerSlots;
        Integer alStripes = rawRef.getParsed(LayerDrbdResources.AL_STRIPES);
        Long alStripeSize = rawRef.getParsed(LayerDrbdResources.AL_STRIPE_SIZE);
        Long initFlags = rawRef.getParsed(LayerDrbdResources.FLAGS);
        switch (getDbType())
        {
            case SQL:
            case K8S_CRD:
                // bug: by accident we store the peerSlots as INTEGER instead of SMALLINT
                peerSlots = rawRef.<Integer>get(LayerDrbdResources.PEER_SLOTS).shortValue();
                break;
            case ETCD:
                peerSlots = rawRef.etcdGetShort(LayerDrbdResources.PEER_SLOTS);
                break;
            default:
                throw new ImplementationError("Unknown db type: " + getDbType());
        }

        DrbdRscData<?> drbdRscData;
        if (currentDummyLoadingRLORef.getSnapName() == null)
        {
            Set<AbsRscLayerObject<Resource>> children = new HashSet<>();
            typeErasedChildren = (Set<AbsRscLayerObject<?>>) ((Object) children);
            Map<VolumeNumber, DrbdVlmData<Resource>> drbdVlmMap = new TreeMap<>();
            typeErasedVlmMap = (Map<VolumeNumber, DrbdVlmData<?>>) ((Object) drbdVlmMap);

            drbdRscData = new DrbdRscData<>(
                lri,
                (Resource) absRscRef,
                (AbsRscLayerObject<Resource>) loadedParentRscDataRef,
                (DrbdRscDfnData<Resource>) getRscDfnData(nodeNameSuffixedRscNamePairRef.objB),
                children,
                drbdVlmMap,
                nodeNameSuffixedRscNamePairRef.objB.rscNameSuffix,
                nodeId,
                peerSlots,
                alStripes,
                alStripeSize,
                initFlags,
                this,
                drbdVlmDriver,
                transObjFactory,
                transMgrProvider
            );
        }
        else
        {
            Set<AbsRscLayerObject<Snapshot>> children = new HashSet<>();
            typeErasedChildren = (Set<AbsRscLayerObject<?>>) ((Object) children);
            Map<VolumeNumber, DrbdVlmData<Snapshot>> drbdVlmMap = new TreeMap<>();
            typeErasedVlmMap = (Map<VolumeNumber, DrbdVlmData<?>>) ((Object) drbdVlmMap);

            drbdRscData = new DrbdRscData<>(
                lri,
                (Snapshot) absRscRef,
                (AbsRscLayerObject<Snapshot>) loadedParentRscDataRef,
                (DrbdRscDfnData<Snapshot>) getRscDfnData(nodeNameSuffixedRscNamePairRef.objB),
                children,
                drbdVlmMap,
                nodeNameSuffixedRscNamePairRef.objB.rscNameSuffix,
                nodeId,
                peerSlots,
                alStripes,
                alStripeSize,
                initFlags,
                this,
                drbdVlmDriver,
                transObjFactory,
                transMgrProvider
            );
        }

        return new RscDataLoadOutput<>(drbdRscData, typeErasedChildren, typeErasedVlmMap);
    }
}
