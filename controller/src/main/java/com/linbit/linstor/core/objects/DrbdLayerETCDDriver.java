package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Column;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerDrbdResourceDefinitions;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerDrbdResources;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerDrbdVolumeDefinitions;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerDrbdVolumes;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgrETCD;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.core.objects.ResourceDefinitionDbDriver.DFLT_SNAP_NAME_FOR_RSC;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Singleton
public class DrbdLayerETCDDriver extends BaseEtcdDriver implements DrbdLayerCtrlDatabaseDriver
{
    private static final String NULL = ":null";
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;
    private final TransactionObjectFactory transObjFactory;
    private final DynamicNumberPool tcpPortPool;
    private final DynamicNumberPool minorPool;

    private final StateFlagsPersistence<DrbdRscData<?>> rscFlagsDriver;
    private final SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool> vlmExtStorPoolDriver;
    private final SingleColumnDatabaseDriver<DrbdRscDfnData<?>, String> rscDfnSecretDriver;
    private final SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TcpPortNumber> rscDfnTcpPortDriver;
    private final SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TransportType> rscDfnTransportTypeDriver;
    private final SingleColumnDatabaseDriver<DrbdRscDfnData<?>, Short> rscDfnPeerSlotDriver;

    private final Map<Pair<ResourceDefinition, String>, Pair<DrbdRscDfnData<Resource>, List<DrbdRscData<Resource>>>> drbdRscDfnCache;
    private final Map<Pair<VolumeDefinition, String>, DrbdVlmDfnData<Resource>> drbdVlmDfnCache;
    private final Map<Pair<SnapshotDefinition, String>, Pair<DrbdRscDfnData<Snapshot>, List<DrbdRscData<Snapshot>>>> drbdSnapDfnCache;
    private final Map<Pair<SnapshotVolumeDefinition, String>, DrbdVlmDfnData<Snapshot>> drbdSnapVlmDfnCache;

    @Inject
    public DrbdLayerETCDDriver(
        @SystemContext AccessContext accCtxRef,
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrETCD> transMgrProviderRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        @Named(NumberPoolModule.MINOR_NUMBER_POOL) DynamicNumberPool minorPoolRef
    )
    {
        super(transMgrProviderRef);
        dbCtx = accCtxRef;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
        tcpPortPool = tcpPortPoolRef;
        minorPool = minorPoolRef;

        rscFlagsDriver = new RscFlagsDriver();
        vlmExtStorPoolDriver = new VlmExtStorPoolDriver();
        rscDfnSecretDriver = new RscDfnSecretDriver();
        rscDfnTcpPortDriver = new RscDfnTcpPortDriver();
        rscDfnTransportTypeDriver = new RscDfnTransportTypeDriver();
        rscDfnPeerSlotDriver = new RscDfnPeerSlotDriver();

        drbdRscDfnCache = new HashMap<>();
        drbdVlmDfnCache = new HashMap<>();
        drbdSnapDfnCache = new HashMap<>();
        drbdSnapVlmDfnCache = new HashMap<>();
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    /*
     * These caches are only used during loading. Having them cleared after having loaded all data
     * these caches should never be used again.
     */
    @Override
    public void clearLoadCache()
    {
        drbdRscDfnCache.clear();
        drbdVlmDfnCache.clear();
    }

    /**
     * Loads the layer data for the given {@link ResourceDefinition}s if available.
     *
     * @param rscDfnMap
     * @throws DatabaseException
     */

    @Override
    public void loadLayerData(
        Map<ResourceName, ResourceDefinition> rscDfnMap,
        Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> snapDfnMap
    )
        throws DatabaseException
    {
        try
        {
            Map<String, String> allDrbdRscDfnMap = namespace(GeneratedDatabaseTables.LAYER_DRBD_RESOURCE_DEFINITIONS)
                .get(true);
            Set<String> composedPkSet = EtcdUtils.getComposedPkList(allDrbdRscDfnMap);
            for (String composedPk : composedPkSet)
            {
                String[] pks = EtcdUtils.splitPks(composedPk, true);
                String rscDfnStr = pks[LayerDrbdResourceDefinitions.RESOURCE_NAME.getIndex()];
                String rscNameSuffix = pks[LayerDrbdResourceDefinitions.RESOURCE_NAME_SUFFIX.getIndex()];
                String snapDfnStr = pks[LayerDrbdResourceDefinitions.SNAPSHOT_NAME.getIndex()];

                ResourceName rscName = new ResourceName(rscDfnStr);

                ResourceDefinition rscDfn;
                SnapshotDefinition snapDfn;
                DrbdRscDfnData<?> drbdRscDfnData;

                boolean restoreAsResourceDefinition = snapDfnStr == null || snapDfnStr.isEmpty();
                if (restoreAsResourceDefinition)
                {
                    rscDfn = rscDfnMap.get(rscName);
                    snapDfn = null;
                    drbdRscDfnData = loadDrbdRscDfn(rscDfn, rscName, rscNameSuffix);
                }
                else
                {
                    rscDfn = null;
                    SnapshotName snapshotName = new SnapshotName(snapDfnStr);
                    snapDfn = snapDfnMap.get(new Pair<>(rscName, snapshotName));
                    drbdRscDfnData = loadDrbdRscDfn(snapDfn, rscName, snapshotName, rscNameSuffix);
                }
                if (drbdRscDfnData == null)
                {
                    short peerSlots = Short.parseShort(
                        get(allDrbdRscDfnMap, LayerDrbdResourceDefinitions.PEER_SLOTS, pks)
                    );
                    int alStripes = Integer.parseInt(
                        get(allDrbdRscDfnMap, LayerDrbdResourceDefinitions.AL_STRIPES, pks)
                    );
                    long alStripeSize = Long.parseLong(
                        get(allDrbdRscDfnMap, LayerDrbdResourceDefinitions.AL_STRIPE_SIZE, pks)
                    );
                    Integer tcpPort;
                    String secret;
                    if (restoreAsResourceDefinition)
                    {
                        tcpPort = Integer.parseInt(
                            get(allDrbdRscDfnMap, LayerDrbdResourceDefinitions.TCP_PORT, pks)
                        );
                        secret = get(allDrbdRscDfnMap, LayerDrbdResourceDefinitions.SECRET, pks);
                    }
                    else
                    {
                        tcpPort = null;
                        secret = null;
                    }

                    TransportType transportType = TransportType.byValue(
                        get(allDrbdRscDfnMap, LayerDrbdResourceDefinitions.TRANSPORT_TYPE, pks)
                    );

                    if (restoreAsResourceDefinition)
                    {
                        restoreAndCache(
                            rscDfn,
                            rscNameSuffix,
                            peerSlots,
                            alStripes,
                            alStripeSize,
                            tcpPort,
                            transportType,
                            secret
                        );
                    }
                    else
                    {
                        restoreAndCache(
                            snapDfn,
                            rscNameSuffix,
                            peerSlots,
                            alStripes,
                            alStripeSize,
                            transportType
                        );
                    }
                }
            }
            Map<String, String> allDrbdVlmDfnMap = namespace(GeneratedDatabaseTables.LAYER_DRBD_VOLUME_DEFINITIONS)
                .get(true);
            Set<String> vlmDfnComposedKeySet = EtcdUtils.getComposedPkList(allDrbdVlmDfnMap);
            for (String vlmDfnComposedKey : vlmDfnComposedKeySet)
            {
                String[] pks = vlmDfnComposedKey.split(EtcdUtils.PK_DELIMITER);
                String rscName = pks[LayerDrbdVolumeDefinitions.RESOURCE_NAME.getIndex()];
                String rscNameSuffix = pks[LayerDrbdVolumeDefinitions.RESOURCE_NAME_SUFFIX.getIndex()];
                String snapDfnStr = pks[LayerDrbdVolumeDefinitions.SNAPSHOT_NAME.getIndex()];
                int vlmNr = Integer.parseInt(pks[LayerDrbdVolumeDefinitions.VLM_NR.getIndex()]);

                if (snapDfnStr == null || snapDfnStr.isEmpty())
                {
                    ResourceDefinition rscDfn = rscDfnMap.get(new ResourceName(rscName));
                    VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(dbCtx, new VolumeNumber(vlmNr));
                    if (vlmDfn == null)
                    {
                        throw new LinStorDBRuntimeException(
                            "Loaded drbd volume definition data for non existent volume definition '" +
                                rscName + "', vlmNr: " + vlmNr
                        );
                    }
                    String minorStr = get(allDrbdVlmDfnMap, LayerDrbdVolumeDefinitions.VLM_MINOR_NR, pks);
                    Integer minor = minorStr == null ? null : Integer.parseInt(minorStr);

                    DrbdVlmDfnData<Resource> drbdVlmDfnData = new DrbdVlmDfnData<Resource>(
                        vlmDfn,
                        null,
                        rscNameSuffix,
                        minor,
                        minorPool,
                        vlmDfn.getResourceDefinition().<DrbdRscDfnData<Resource>> getLayerData(
                            dbCtx,
                            DeviceLayerKind.DRBD,
                            rscNameSuffix
                        ),
                        this,
                        transMgrProvider
                    );
                    drbdVlmDfnCache.put(new Pair<>(vlmDfn, rscNameSuffix), drbdVlmDfnData);

                    vlmDfn.setLayerData(dbCtx, drbdVlmDfnData);
                }
                else
                {
                    SnapshotDefinition snapDfn = snapDfnMap.get(
                        new Pair<>(
                            new ResourceName(rscName),
                            new SnapshotName(snapDfnStr)
                        )
                    );
                    SnapshotVolumeDefinition snapVlmDfn = snapDfn
                        .getSnapshotVolumeDefinition(dbCtx, new VolumeNumber(vlmNr));
                    if (snapVlmDfn == null)
                    {
                        throw new LinStorDBRuntimeException(
                            "Loaded drbd snapshot volume definition data for non existent snapshot volume definition '"
                                +
                                rscName + "', '" + snapDfnStr + "', vlmNr: " + vlmNr
                        );
                    }
                    DrbdVlmDfnData<Snapshot> drbdSnapVlmDfnData = new DrbdVlmDfnData<Snapshot>(
                        snapVlmDfn.getVolumeDefinition(),
                        snapVlmDfn.getSnapshotName(),
                        rscNameSuffix,
                        DrbdVlmDfnData.SNAPSHOT_MINOR,
                        minorPool,
                        snapVlmDfn.getSnapshotDefinition().<DrbdRscDfnData<Snapshot>> getLayerData(
                            dbCtx,
                            DeviceLayerKind.DRBD,
                            rscNameSuffix
                        ),
                        this,
                        transMgrProvider
                    );
                    drbdSnapVlmDfnCache.put(new Pair<>(snapVlmDfn, rscNameSuffix), drbdSnapVlmDfnData);

                    snapVlmDfn.setLayerData(dbCtx, drbdSnapVlmDfnData);
                }
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new LinStorDBRuntimeException(
                "Failed to restore stored resourceName [" + invalidNameExc.invalidName + "]",
                invalidNameExc
            );
        }
        catch (ValueOutOfRangeException | ExhaustedPoolException | ValueInUseException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
        }
    }

    private String get(Map<String, String> map, Column col, String... pks)
    {
        return map.get(EtcdUtils.buildKey(col, pks));
    }

    private DrbdRscDfnData<Resource> loadDrbdRscDfn(
        ResourceDefinition rscDfn,
        ResourceName rscName,
        String rscNameSuffixRef
    )
        throws AccessDeniedException
    {
        if (rscDfn == null)
        {
            throw new LinStorDBRuntimeException(
                "Loaded drbd resource definition data for non existent resource definition '" +
                    rscName + "'"
            );
        }
        return rscDfn.getLayerData(
            dbCtx,
            DeviceLayerKind.DRBD,
            rscNameSuffixRef
        );
    }

    private void restoreAndCache(
        ResourceDefinition rscDfn,
        String rscNameSuffix,
        short peerSlots,
        int alStripes,
        long alStripeSize,
        int tcpPort,
        TransportType transportType,
        String secret
    )
        throws ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException, AccessDeniedException,
        DatabaseException
    {
        List<DrbdRscData<Resource>> rscDataList = new ArrayList<>();

        DrbdRscDfnData<Resource> drbdRscDfnData = new DrbdRscDfnData<>(
            rscDfn.getName(),
            null,
            rscNameSuffix,
            peerSlots,
            alStripes,
            alStripeSize,
            tcpPort,
            transportType,
            secret,
            rscDataList,
            new TreeMap<>(),
            tcpPortPool,
            this,
            transObjFactory,
            transMgrProvider
        );

        Pair<DrbdRscDfnData<Resource>, List<DrbdRscData<Resource>>> pair = new Pair<>(
            drbdRscDfnData,
            rscDataList
        );
        drbdRscDfnCache.put(new Pair<>(rscDfn, rscNameSuffix), pair);

        rscDfn.setLayerData(dbCtx, drbdRscDfnData);
    }

    private DrbdRscDfnData<Snapshot> loadDrbdRscDfn(
        SnapshotDefinition snapDfn,
        ResourceName rscName,
        SnapshotName snapshotName,
        String rscNameSuffix
    )
        throws AccessDeniedException
    {
        if (snapDfn == null)
        {
            throw new LinStorDBRuntimeException(
                "Loaded drbd snapshot definition data for non existent snapshot definition '" +
                    rscName + "', '" + snapshotName + "'"
            );
        }
        return snapDfn.getLayerData(
            dbCtx,
            DeviceLayerKind.DRBD,
            rscNameSuffix
        );
    }

    private void restoreAndCache(
        SnapshotDefinition snapDfn,
        String rscNameSuffix,
        short peerSlots,
        int alStripes,
        long alStripeSize,
        TransportType transportType
    )
        throws ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException, AccessDeniedException,
        DatabaseException
    {
        List<DrbdRscData<Snapshot>> rscDataList = new ArrayList<>();

        DrbdRscDfnData<Snapshot> drbdSnapDfnData = new DrbdRscDfnData<>(
            snapDfn.getResourceName(),
            snapDfn.getName(),
            rscNameSuffix,
            peerSlots,
            alStripes,
            alStripeSize,
            DrbdRscDfnData.SNAPSHOT_TCP_PORT,
            transportType,
            null,
            rscDataList,
            new TreeMap<>(),
            tcpPortPool,
            this,
            transObjFactory,
            transMgrProvider
        );

        Pair<DrbdRscDfnData<Snapshot>, List<DrbdRscData<Snapshot>>> pair = new Pair<>(
            drbdSnapDfnData,
            rscDataList
        );
        drbdSnapDfnCache.put(new Pair<>(snapDfn, rscNameSuffix), pair);

        snapDfn.setLayerData(dbCtx, drbdSnapDfnData);
    }


    @Override
    public <RSC extends AbsResource<RSC>> Pair<DrbdRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRsc,
        int id,
        String rscSuffix,
        AbsRscLayerObject<RSC> absParent,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> storPoolMap
    )
        throws DatabaseException
    {
        Pair<DrbdRscData<RSC>, Set<AbsRscLayerObject<RSC>>> ret;
        String idString = Integer.toString(id);
        Map<String, String> rscDataMap = namespace(GeneratedDatabaseTables.LAYER_DRBD_RESOURCES, idString)
            .get(true);

        if (!rscDataMap.isEmpty())
        {
            NodeId nodeId;
            try
            {
                nodeId = new NodeId(
                    Integer.parseInt(
                        get(rscDataMap, LayerDrbdResources.NODE_ID, idString)
                    )
                );
            }
            catch (ValueOutOfRangeException exc)
            {
                throw new LinStorDBRuntimeException(
                    "Failed to restore stored nodeId [" + get(rscDataMap, LayerDrbdResources.NODE_ID, idString) + "]"
                );
            }
            String peerSlotsStr = get(rscDataMap, LayerDrbdResources.PEER_SLOTS, idString);
            Short peerSlots = peerSlotsStr == null ? null : Short.parseShort(peerSlotsStr);

            String alStripesStr = get(rscDataMap, LayerDrbdResources.AL_STRIPES, idString);
            Integer alStripes = alStripesStr == null ? null : Integer.parseInt(alStripesStr);

            String alStripeSizeStr = get(rscDataMap, LayerDrbdResources.AL_STRIPE_SIZE, idString);
            Long alStripeSize = alStripeSizeStr == null ? null : Long.parseLong(alStripeSizeStr);

            long initFlags = Long.parseLong(get(rscDataMap, LayerDrbdResources.FLAGS, idString));

            Set<AbsRscLayerObject<RSC>> children = new HashSet<>();
            Object childrenAfterTypeEreasure = children; // sorry for this hack

            if (absRsc instanceof Resource)
            {
                Resource rsc = (Resource) absRsc;
                Pair<DrbdRscDfnData<Resource>, List<DrbdRscData<Resource>>> drbdRscDfnDataPair = drbdRscDfnCache.get(
                    new Pair<>(rsc.getDefinition(), rscSuffix)
                );

                Map<VolumeNumber, DrbdVlmData<Resource>> vlmMap = new TreeMap<>();

                DrbdRscData<Resource> drbdRscData = new DrbdRscData<Resource>(
                    id,
                    rsc,
                    (AbsRscLayerObject<Resource>) absParent,
                    drbdRscDfnDataPair.objA,
                    (Set<AbsRscLayerObject<Resource>>) childrenAfterTypeEreasure,
                    vlmMap,
                    rscSuffix,
                    nodeId,
                    peerSlots,
                    alStripes,
                    alStripeSize,
                    initFlags,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                ret = new Pair<DrbdRscData<RSC>, Set<AbsRscLayerObject<RSC>>>(
                    (DrbdRscData<RSC>) drbdRscData,
                    children
                );
                drbdRscDfnDataPair.objB.add(drbdRscData);

                restoreDrbdVolumes(drbdRscData, vlmMap, storPoolMap);
            }
            else
            {
                Snapshot snap = (Snapshot) absRsc;
                Pair<DrbdRscDfnData<Snapshot>, List<DrbdRscData<Snapshot>>> drbdSnapDfnDataPair = drbdSnapDfnCache.get(
                    new Pair<>(snap.getSnapshotDefinition(), rscSuffix)
                );

                Map<VolumeNumber, DrbdVlmData<Snapshot>> vlmMap = new TreeMap<>();
                DrbdRscData<Snapshot> drbdSnapData = new DrbdRscData<Snapshot>(
                    id,
                    snap,
                    (AbsRscLayerObject<Snapshot>) absParent,
                    drbdSnapDfnDataPair.objA,
                    (Set<AbsRscLayerObject<Snapshot>>) childrenAfterTypeEreasure,
                    vlmMap,
                    rscSuffix,
                    nodeId,
                    peerSlots,
                    alStripes,
                    alStripeSize,
                    initFlags,
                    this,
                    transObjFactory,
                    transMgrProvider
                );
                ret = new Pair<DrbdRscData<RSC>, Set<AbsRscLayerObject<RSC>>>(
                    (DrbdRscData<RSC>) drbdSnapData,
                    children
                );

                drbdSnapDfnDataPair.objB.add(drbdSnapData);

                restoreDrbdVolumes(drbdSnapData, vlmMap, storPoolMap);
            }
        }
        else
        {
            throw new ImplementationError("Requested id [" + id + "] was not found in the database");
        }
        return ret;
    }

    private <RSC extends AbsResource<RSC>> void restoreDrbdVolumes(
        DrbdRscData<RSC> rscData,
        Map<VolumeNumber, DrbdVlmData<RSC>> vlmMap,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> storPoolMapRef
    ) throws DatabaseException
    {
        RSC rsc = rscData.getAbsResource();
        NodeName currentNodeName = rsc.getNode().getName();

        int vlmNrInt = -1;
        try
        {
            Map<String, String> drbdVlmMap = namespace(GeneratedDatabaseTables.LAYER_DRBD_VOLUMES)
                .get(true);
            Set<String> composedPkSet = EtcdUtils.getComposedPkList(drbdVlmMap);
            for (String composedPk : composedPkSet)
            {
                String[] pks = composedPk.split(EtcdUtils.PK_DELIMITER);

                vlmNrInt = Integer.parseInt(pks[LayerDrbdVolumes.VLM_NR.getIndex()]);
                String extMetaStorPoolNameStr = get(drbdVlmMap, LayerDrbdVolumes.POOL_NAME, pks);

                VolumeNumber vlmNr = new VolumeNumber(vlmNrInt);

                StorPool extMetaDataStorPool = null;
                if (extMetaStorPoolNameStr != null && !extMetaStorPoolNameStr.equalsIgnoreCase(NULL))
                {
                    StorPoolName extStorPoolName = new StorPoolName(extMetaStorPoolNameStr);
                    extMetaDataStorPool = storPoolMapRef.get(
                        new Pair<>(currentNodeName, extStorPoolName)
                    ).objA;
                }

                if (rsc instanceof Resource)
                {
                    Volume vlm = ((Resource) rsc).getVolume(vlmNr);

                    VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
                    DrbdVlmDfnData<Resource> drbdVlmDfnData = drbdVlmDfnCache.get(
                        new Pair<>(vlmDfn, rscData.getResourceNameSuffix())
                    );
                    if (drbdVlmDfnData != null)
                    {
                        vlmMap.put(
                            vlmDfn.getVolumeNumber(),
                            (DrbdVlmData<RSC>) new DrbdVlmData<Resource>(
                                vlm,
                                (DrbdRscData<Resource>) rscData,
                                drbdVlmDfnData,
                                extMetaDataStorPool,
                                this,
                                transObjFactory,
                                transMgrProvider
                            )
                        );
                    }
                    else
                    {
                        throw new ImplementationError(
                            "Trying to load drbdVlm without drbdVlmDfn. " +
                                "layerId: " + rscData.getRscLayerId() + " node name: " + currentNodeName +
                                " resource name: " + vlmDfn.getResourceDefinition().getName() + " vlmNr " + vlmNrInt
                        );
                    }
                }
                else
                {
                    SnapshotVolume snapVlm = ((Snapshot) rsc).getVolume(vlmNr);

                    SnapshotVolumeDefinition snapVlmDfn = snapVlm.getSnapshotVolumeDefinition();
                    DrbdVlmDfnData<Snapshot> drbdSnapVlmDfnData = drbdSnapVlmDfnCache.get(
                        new Pair<>(snapVlmDfn, rscData.getResourceNameSuffix())
                    );
                    if (drbdSnapVlmDfnData != null)
                    {
                        vlmMap.put(
                            snapVlmDfn.getVolumeNumber(),
                            (DrbdVlmData<RSC>) new DrbdVlmData<Snapshot>(
                                snapVlm,
                                (DrbdRscData<Snapshot>) rscData,
                                drbdSnapVlmDfnData,
                                extMetaDataStorPool,
                                this,
                                transObjFactory,
                                transMgrProvider
                            )
                        );
                    }
                    else
                    {
                        throw new ImplementationError(
                            "Trying to load drbdSnapVlm without drbdSnapVlmDfn. " +
                                "layerId: " + rscData.getRscLayerId() +
                                " node name: " + currentNodeName +
                                " resource name: " + snapVlmDfn.getResourceName() +
                                " snapshot name: " + snapVlmDfn.getSnapshotName() +
                                " vlmNr " + vlmNrInt
                            );
                    }
                }
            }
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new LinStorDBRuntimeException(
                "Failed to restore stored volume number " + vlmNrInt +
                    " for resource layer id: " + rscData.getRscLayerId()
            );
        }
        catch (InvalidNameException exc)
        {
            throw new LinStorDBRuntimeException(
                "Failed to restore stored storage pool name '" + exc.invalidName +
                    "' for resource layer id " + rscData.getRscLayerId() + " vlmNr: " + vlmNrInt
            );
        }
    }

    @Override
    public void create(DrbdRscData<?> drbdRscData) throws DatabaseException
    {
        errorReporter.logTrace("Creating DrbdRscData %s", getId(drbdRscData));
        try
        {
            getNamespace(drbdRscData)
                .put(LayerDrbdResources.PEER_SLOTS, Short.toString(drbdRscData.getPeerSlots()))
                .put(LayerDrbdResources.AL_STRIPES, Integer.toString(drbdRscData.getAlStripes()))
                .put(LayerDrbdResources.AL_STRIPE_SIZE, Long.toString(drbdRscData.getAlStripeSize()))
                .put(LayerDrbdResources.FLAGS, Long.toString(drbdRscData.getFlags().getFlagsBits(dbCtx)))
                .put(LayerDrbdResources.NODE_ID, Integer.toString(drbdRscData.getNodeId().value));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    public void delete(DrbdRscData<?> drbdRscData) throws DatabaseException
    {
        errorReporter.logTrace("Deleting DrbdRscDataRef %s", getId(drbdRscData));
        getNamespace(drbdRscData)
            .delete(true);
    }

    @Override
    public StateFlagsPersistence<DrbdRscData<?>> getRscStateFlagPersistence()
    {
        return rscFlagsDriver;
    }

    @Override
    public void persist(DrbdRscDfnData<?> drbdRscDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Creating DrbdRscDfnData %s", getId(drbdRscDfnData));

        FluentLinstorTransaction tx = getNamespace(drbdRscDfnData)
            .put(LayerDrbdResourceDefinitions.PEER_SLOTS, Short.toString(drbdRscDfnData.getPeerSlots()))
            .put(LayerDrbdResourceDefinitions.AL_STRIPES, Integer.toString(drbdRscDfnData.getAlStripes()))
            .put(LayerDrbdResourceDefinitions.AL_STRIPE_SIZE, Long.toString(drbdRscDfnData.getAlStripeSize()))
            .put(LayerDrbdResourceDefinitions.TRANSPORT_TYPE, drbdRscDfnData.getTransportType().name());
        if (drbdRscDfnData.getTcpPort() != null)
        {
            tx.put(LayerDrbdResourceDefinitions.TCP_PORT, Integer.toString(drbdRscDfnData.getTcpPort().value));
        }
        if (drbdRscDfnData.getSecret() != null)
        {
            tx.put(LayerDrbdResourceDefinitions.SECRET, drbdRscDfnData.getSecret());
        }
    }

    @Override
    public void delete(DrbdRscDfnData<?> drbdRscDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Deleting DrbdRscDfnData %s", getId(drbdRscDfnData));
        getNamespace(drbdRscDfnData)
            .delete(true);
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TcpPortNumber> getTcpPortDriver()
    {
        return rscDfnTcpPortDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TransportType> getTransportTypeDriver()
    {
        return rscDfnTransportTypeDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, String> getRscDfnSecretDriver()
    {
        return rscDfnSecretDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, Short> getPeerSlotsDriver()
    {
        return rscDfnPeerSlotDriver;
    }

    @Override
    public void persist(DrbdVlmData<?> drbdVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating DrbdVlmData %s", getId(drbdVlmDataRef));
        StorPool extStorPool = drbdVlmDataRef.getExternalMetaDataStorPool();
        if (extStorPool == null)
        {
            // TODO: needs improvement
            getNamespace(drbdVlmDataRef)
                .put(LayerDrbdVolumes.NODE_NAME, NULL)
                .put(LayerDrbdVolumes.POOL_NAME, NULL);
        }
        else
        {
            getNamespace(drbdVlmDataRef)
                .put(LayerDrbdVolumes.NODE_NAME, extStorPool.getNode().getName().value)
                .put(LayerDrbdVolumes.POOL_NAME, extStorPool.getName().value);
        }
    }

    @Override
    public void delete(DrbdVlmData<?> drbdVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting DrbdVlmData %s", getId(drbdVlmDataRef));
        getNamespace(drbdVlmDataRef)
            .delete(true);
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool> getExtStorPoolDriver()
    {
        return vlmExtStorPoolDriver;
    }

    @Override
    public void persist(DrbdVlmDfnData<?> drbdVlmDfnDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating DrbdVlmDfnData %s", getId(drbdVlmDfnDataRef));

        MinorNumber minorNr = drbdVlmDfnDataRef.getMinorNr();
        if (minorNr != null)
        {
            getNamespace(drbdVlmDfnDataRef)
                .put(LayerDrbdVolumeDefinitions.VLM_MINOR_NR, minorNr.toString());
        }
        else
        {
            getNamespace(drbdVlmDfnDataRef)
                .put(LayerDrbdVolumeDefinitions.VLM_MINOR_NR, NULL);
        }
    }

    @Override
    public void delete(DrbdVlmDfnData<?> drbdVlmDfnDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting DrbdVlmDfnData %s", getId(drbdVlmDfnDataRef));
        getNamespace(drbdVlmDfnDataRef)
            .delete(true);
    }

    private FluentLinstorTransaction getNamespace(DrbdRscData<?> drbdRscData)
    {
        return namespace(GeneratedDatabaseTables.LAYER_DRBD_RESOURCES, Integer.toString(drbdRscData.getRscLayerId()));
    }

    private FluentLinstorTransaction getNamespace(DrbdRscDfnData<?> drbdRscDfnData)
    {
        return namespace(
            GeneratedDatabaseTables.LAYER_DRBD_RESOURCE_DEFINITIONS,
            drbdRscDfnData.getResourceName().value,
            drbdRscDfnData.getRscNameSuffix(),
            snapshotNameToEctdKey(drbdRscDfnData.getSnapshotName())
        );
    }

    private FluentLinstorTransaction getNamespace(DrbdVlmData<?> drbdVlmDataRef)
    {
        return namespace(
            GeneratedDatabaseTables.LAYER_DRBD_VOLUMES,
            Integer.toString(drbdVlmDataRef.getRscLayerId()),
            Integer.toString(drbdVlmDataRef.getVlmNr().value)
        );
    }

    private FluentLinstorTransaction getNamespace(DrbdVlmDfnData<?> drbdVlmDfnDataRef)
    {
        return namespace(
            GeneratedDatabaseTables.LAYER_DRBD_VOLUME_DEFINITIONS,
            drbdVlmDfnDataRef.getVolumeDefinition().getResourceDefinition().getName().value,
            drbdVlmDfnDataRef.getRscNameSuffix(),
            snapshotNameToEctdKey(drbdVlmDfnDataRef.getSnapshotName()),
            Integer.toString(drbdVlmDfnDataRef.getVolumeDefinition().getVolumeNumber().value)
        );
    }

    private String snapshotNameToEctdKey(SnapshotName snapName)
    {
        String ret;
        if (snapName == null || snapName.value.trim().isEmpty())
        {
            ret = DFLT_SNAP_NAME_FOR_RSC;
        }
        else
        {
            ret = snapName.value;
        }
        return ret;
    }

    private String getId(DrbdVlmData<?> drbdVlmData)
    {
        return "(LayerRscId=" + drbdVlmData.getRscLayerId() +
            ", VlmNr=" + drbdVlmData.getVlmNr() + ")";
    }

    private String getId(DrbdRscData<?> drbdRscData)
    {
        return "(LayerRscId=" + drbdRscData.getRscLayerId() + ")";
    }

    private String getId(DrbdRscDfnData<?> drbdRscDfnData)
    {
        return "(ResName=" + drbdRscDfnData.getResourceName() +
            ", ResNameSuffix=" + drbdRscDfnData.getRscNameSuffix() +
            ", SnapName=" + drbdRscDfnData.getSnapshotName() + ")";
    }

    private String getId(DrbdVlmDfnData<?> drbdVlmDfnData)
    {
        return "(ResName=" + drbdVlmDfnData.getResourceName() +
            ", ResNameSuffix=" + drbdVlmDfnData.getRscNameSuffix() +
            ", SnapName=" + drbdVlmDfnData.getSnapshotName() +
            ", VlmNr=" + drbdVlmDfnData.getVolumeDefinition().getVolumeNumber().value + ")";
    }

    private class RscFlagsDriver implements StateFlagsPersistence<DrbdRscData<?>>
    {

        @Override
        public void persist(DrbdRscData<?> drbdRscData, long flags) throws DatabaseException
        {
            try
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        DrbdRscObject.DrbdRscFlags.class,
                        drbdRscData.getFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(DrbdRscObject.DrbdRscFlags.class, flags),
                    ", "
                );
                String inlineId = getId(drbdRscData);

                errorReporter
                    .logTrace(
                        "Updating DrbdRscData's flags from [%s] to [%s] %s",
                        fromFlags,
                        toFlags,
                        inlineId
                    );
                getNamespace(drbdRscData)
                    .put(LayerDrbdResources.FLAGS, Long.toString(flags));
            }
            catch (AccessDeniedException exc)
            {
                DatabaseLoader.handleAccessDeniedException(exc);

            }
        }
    }

    private class VlmExtStorPoolDriver implements SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool>
    {
        @Override
        public void update(DrbdVlmData<?> drbdVlmData, StorPool extStorPool) throws DatabaseException
        {
            String fromStr = null;
            String toStr = null;
            if (drbdVlmData.getExternalMetaDataStorPool() != null)
            {
                fromStr = drbdVlmData.getExternalMetaDataStorPool().getName().displayValue;
            }
            if (extStorPool != null)
            {
                toStr = extStorPool.getName().displayValue;
            }
            errorReporter.logTrace(
                "Updating DrbdVlmData's external storage pool from [%s] to [%s] %s",
                fromStr,
                toStr,
                getId(drbdVlmData)
            );
            if (extStorPool == null)
            {
                // TODO: needs improvement
                getNamespace(drbdVlmData)
                    .put(LayerDrbdVolumes.NODE_NAME, "null")
                    .put(LayerDrbdVolumes.POOL_NAME, "null");
            }
            else
            {
                getNamespace(drbdVlmData)
                    .put(LayerDrbdVolumes.NODE_NAME, extStorPool.getNode().getName().value)
                    .put(LayerDrbdVolumes.POOL_NAME, extStorPool.getName().value);
            }
        }
    }

    private class RscDfnSecretDriver implements SingleColumnDatabaseDriver<DrbdRscDfnData<?>, String>
    {

        @Override
        public void update(DrbdRscDfnData<?> drbdRscDfnData, String secretRef) throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating DrbdRscDfnData's secret from [%s] to [%s] %s",
                drbdRscDfnData.getSecret(),
                secretRef,
                getId(drbdRscDfnData)
            );
            getNamespace(drbdRscDfnData)
                .put(LayerDrbdResourceDefinitions.SECRET, drbdRscDfnData.getSecret());
        }

    }

    private class RscDfnTcpPortDriver
        implements SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TcpPortNumber>
    {

        @Override
        public void update(DrbdRscDfnData<?> drbdRscDfnData, TcpPortNumber port) throws DatabaseException
        {
            TcpPortNumber tcpPort = drbdRscDfnData.getTcpPort();
            errorReporter.logTrace(
                "Updating DrbdRscDfnData's port from [%d] to [%d] %s",
                tcpPort,
                port,
                getId(drbdRscDfnData)
            );
            getNamespace(drbdRscDfnData)
                .put(LayerDrbdResourceDefinitions.TCP_PORT, Integer.toString(drbdRscDfnData.getTcpPort().value));
        }

    }

    private class RscDfnTransportTypeDriver
        implements SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TransportType>
    {

        @Override
        public void update(DrbdRscDfnData<?> drbdRscDfnData, TransportType transportType)
            throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating DrbdRscDfnData's transport type from [%s] to [%s] %s",
                drbdRscDfnData.getTransportType().name(),
                transportType.name(),
                getId(drbdRscDfnData)
            );
            getNamespace(drbdRscDfnData)
                .put(LayerDrbdResourceDefinitions.TRANSPORT_TYPE, drbdRscDfnData.getTransportType().name());
        }

    }

    private class RscDfnPeerSlotDriver implements SingleColumnDatabaseDriver<DrbdRscDfnData<?>, Short>
    {
        @Override
        public void update(DrbdRscDfnData<?> drbdRscDfnData, Short peerSlots) throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating DrbdRscDfnData's peer slots from [%d] to [%d] %s",
                drbdRscDfnData.getPeerSlots(),
                peerSlots,
                getId(drbdRscDfnData)
            );
            getNamespace(drbdRscDfnData)
                .put(LayerDrbdResourceDefinitions.PEER_SLOTS, Short.toString(drbdRscDfnData.getPeerSlots()));
        }

    }
}
