package com.linbit.linstor.core.objects;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.db.utils.K8sCrdUtils;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.LayerDrbdResourcesSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.LayerDrbdVolumesSpec;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.K8sCrdTransaction;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

@Singleton
public class DrbdLayerK8sCrdDriver implements DrbdLayerCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;
    private final TransactionObjectFactory transObjFactory;
    private final DynamicNumberPool tcpPortPool;
    private final DynamicNumberPool minorPool;

    private final StateFlagsPersistence<DrbdRscData<?>> rscFlagsDriver;
    private final SingleColumnDatabaseDriver<DrbdRscData<?>, NodeId> rscNodeIdDriver;
    private final SingleColumnDatabaseDriver<DrbdVlmData<?>, StorPool> vlmExtStorPoolDriver;
    private final SingleColumnDatabaseDriver<DrbdRscDfnData<?>, ?> rscDfnUpdateDriver;

    /**
     * The {@link #load(AbsResource, int, String, AbsRscLayerObject, Map)} method is called for every layerId. In order
     * to prevent fetching the full list of LayerDrbdRsc entries from k8s, picking out the one we are interested in for
     * every call of this method, we simply cache the list and filter manually.
     * This cache is cleared once the driver finished loading everything
     */
    private final HashMap<Integer, GenCrdCurrent.LayerDrbdResourcesSpec> drbdRscCache;
    /**
     * The {@link #load(AbsResource, int, String, AbsRscLayerObject, Map)} method is called for every layerId. In order
     * to prevent fetching the full list of LayerDrbdRsc entries from k8s, picking out the one we are interested in for
     * every call of this method, we simply cache the list and filter manually.
     * This cache is cleared once the driver finished loading everything
     */
    private final HashMap<Integer, HashMap<Integer, GenCrdCurrent.LayerDrbdVolumesSpec>> drbdVlmCache;
    private final Map<Pair<ResourceDefinition, String>, Pair<DrbdRscDfnData<Resource>, List<DrbdRscData<Resource>>>> drbdRscDfnCache;
    private final Map<Pair<VolumeDefinition, String>, DrbdVlmDfnData<Resource>> drbdVlmDfnCache;
    private final Map<Pair<SnapshotDefinition, String>, Pair<DrbdRscDfnData<Snapshot>, List<DrbdRscData<Snapshot>>>> drbdSnapDfnCache;
    private final Map<Pair<SnapshotVolumeDefinition, String>, DrbdVlmDfnData<Snapshot>> drbdSnapVlmDfnCache;

    private final Provider<TransactionMgrK8sCrd> transMgrProvider;


    @Inject
    public DrbdLayerK8sCrdDriver(
        @SystemContext AccessContext accCtxRef,
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrK8sCrd> transMgrProviderRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        @Named(NumberPoolModule.MINOR_NUMBER_POOL) DynamicNumberPool minorPoolRef
    )
    {
        dbCtx = accCtxRef;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        tcpPortPool = tcpPortPoolRef;
        minorPool = minorPoolRef;

        rscFlagsDriver = (rscData, ignored) -> update(rscData);
        rscNodeIdDriver = (rscData, ignored) -> update(rscData);
        vlmExtStorPoolDriver = (vlmData, ignored) -> update(vlmData);
        rscDfnUpdateDriver = (rscDfnData, ignored) -> update(rscDfnData);

        drbdRscCache = new HashMap<>();
        drbdVlmCache = new HashMap<>();
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
    public void clearLoadAllCache()
    {
        drbdRscCache.clear();
        drbdVlmCache.clear();
        drbdRscDfnCache.clear();
        drbdVlmDfnCache.clear();
        drbdSnapDfnCache.clear();
        drbdSnapVlmDfnCache.clear();
    }

    /**
     * Loads the layer data for the given {@link ResourceDefinition}s if available.
     *
     * @param rscDfnMap
     * @throws DatabaseException
     */

    @Override
    public void fetchForLoadAll(
        Map<ResourceName, ResourceDefinition> rscDfnMap,
        Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> snapDfnMap
    )
        throws DatabaseException
    {
        try
        {
            K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
            {
                Map<String, GenCrdCurrent.LayerDrbdResourcesSpec> drbdRscSpecList = tx.get(
                    GeneratedDatabaseTables.LAYER_DRBD_RESOURCES
                );
                for (GenCrdCurrent.LayerDrbdResourcesSpec drbdRscSpec : drbdRscSpecList.values())
                {
                    drbdRscCache.put(drbdRscSpec.layerResourceId, drbdRscSpec);
                }
            }

            {
                Map<String, GenCrdCurrent.LayerDrbdVolumesSpec> drbdVlmSpecList = tx.get(
                    GeneratedDatabaseTables.LAYER_DRBD_VOLUMES
                );
                for (GenCrdCurrent.LayerDrbdVolumesSpec drbdVlmSpec : drbdVlmSpecList.values())
                {
                    HashMap<Integer, LayerDrbdVolumesSpec> vlmMap = drbdVlmCache.get(drbdVlmSpec.layerResourceId);
                    if (vlmMap == null)
                    {
                        vlmMap = new HashMap<>();
                        drbdVlmCache.put(drbdVlmSpec.layerResourceId, vlmMap);
                    }
                    vlmMap.put(drbdVlmSpec.vlmNr, drbdVlmSpec);
                }
            }

            Map<String, GenCrdCurrent.LayerDrbdResourceDefinitionsSpec> drbdRscDfnSpecList = tx.get(
                GeneratedDatabaseTables.LAYER_DRBD_RESOURCE_DEFINITIONS
            );

            for (GenCrdCurrent.LayerDrbdResourceDefinitionsSpec drbdRscDfnSpec : drbdRscDfnSpecList.values())
            {
                String rscDfnStr = drbdRscDfnSpec.resourceName;
                String rscNameSuffix = drbdRscDfnSpec.resourceNameSuffix;
                String snapDfnStr = drbdRscDfnSpec.snapshotName;

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
                    short peerSlots = (short) drbdRscDfnSpec.peerSlots;
                    int alStripes = drbdRscDfnSpec.alStripes;
                    long alStripeSize = drbdRscDfnSpec.alStripeSize;
                    Integer tcpPort;
                    String secret;
                    TransportType transportType = TransportType.byValue(drbdRscDfnSpec.transportType);
                    if (restoreAsResourceDefinition)
                    {
                        tcpPort = drbdRscDfnSpec.tcpPort;
                        secret = drbdRscDfnSpec.secret;
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
                        tcpPort = null;
                        secret = null;
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

            Map<String, GenCrdCurrent.LayerDrbdVolumeDefinitionsSpec> drbdVlmDfnSpecMap = tx.get(
                GeneratedDatabaseTables.LAYER_DRBD_VOLUME_DEFINITIONS
            );

            for (GenCrdCurrent.LayerDrbdVolumeDefinitionsSpec drbdVlmDfnSpec : drbdVlmDfnSpecMap.values())
            {
                String rscName = drbdVlmDfnSpec.resourceName;
                String rscNameSuffix = drbdVlmDfnSpec.resourceNameSuffix;
                String snapDfnStr = drbdVlmDfnSpec.snapshotName;
                int vlmNr = drbdVlmDfnSpec.vlmNr;

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
                    Integer minor = drbdVlmDfnSpec.vlmMinorNr;

                    DrbdVlmDfnData<Resource> drbdVlmDfnData = new DrbdVlmDfnData<>(
                        vlmDfn,
                        rscDfn.getName(),
                        null,
                        rscNameSuffix,
                        vlmDfn.getVolumeNumber(),
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
                    SnapshotVolumeDefinition snapVlmDfn = snapDfn.getSnapshotVolumeDefinition(
                        dbCtx,
                        new VolumeNumber(vlmNr)
                    );
                    if (snapVlmDfn == null)
                    {
                        throw new LinStorDBRuntimeException(
                            "Loaded drbd snapshot volume definition data for non existent snapshot volume definition '"
                                +
                                rscName + "', '" + snapDfnStr + "', vlmNr: " + vlmNr
                        );
                    }
                    DrbdVlmDfnData<Snapshot> drbdSnapVlmDfnData = new DrbdVlmDfnData<>(
                        snapVlmDfn.getVolumeDefinition(),
                        snapDfn.getResourceName(),
                        snapVlmDfn.getSnapshotName(),
                        rscNameSuffix,
                        snapVlmDfn.getVolumeNumber(),
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

        LayerDrbdResourcesSpec drbdRscSpec = drbdRscCache.get(id);

        NodeId nodeId;
        try
        {
            nodeId = new NodeId(drbdRscSpec.nodeId);
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new LinStorDBRuntimeException(
                "Failed to restore stored nodeId [" + drbdRscSpec.nodeId + "]"
            );
        }
        Short peerSlots = (short) drbdRscSpec.peerSlots;
        Integer alStripes = drbdRscSpec.alStripes;
        Long alStripeSize = drbdRscSpec.alStripeSize;
        long initFlags = drbdRscSpec.flags;

        Set<AbsRscLayerObject<RSC>> children = new HashSet<>();
        Object childrenAfterTypeEreasure = children; // sorry for this hack

        if (absRsc instanceof Resource)
        {
            Resource rsc = (Resource) absRsc;
            Pair<DrbdRscDfnData<Resource>, List<DrbdRscData<Resource>>> drbdRscDfnDataPair = drbdRscDfnCache.get(
                new Pair<>(rsc.getDefinition(), rscSuffix)
            );

            Map<VolumeNumber, DrbdVlmData<Resource>> vlmMap = new TreeMap<>();

            DrbdRscData<Resource> drbdRscData = new DrbdRscData<>(
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
            ret = new Pair<>(
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
            DrbdRscData<Snapshot> drbdSnapData = new DrbdRscData<>(
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
            ret = new Pair<>(
                (DrbdRscData<RSC>) drbdSnapData,
                children
            );

            drbdSnapDfnDataPair.objB.add(drbdSnapData);

            restoreDrbdVolumes(drbdSnapData, vlmMap, storPoolMap);
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
            Map<Integer, LayerDrbdVolumesSpec> cachedDrbdVlmMap = K8sCrdUtils.getCheckedVlmMap(
                dbCtx,
                rsc,
                drbdVlmCache,
                rscData.getRscLayerId()
            );
            for (Entry<Integer, LayerDrbdVolumesSpec> entry : cachedDrbdVlmMap.entrySet())
            {
                vlmNrInt = entry.getKey();
                GenCrdCurrent.LayerDrbdVolumesSpec drbdVlmSpec = entry.getValue();

                String extMetaStorPoolNameStr = drbdVlmSpec.poolName;

                VolumeNumber vlmNr = new VolumeNumber(vlmNrInt);

                StorPool extMetaDataStorPool = null;
                if (extMetaStorPoolNameStr != null)
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
                            (DrbdVlmData<RSC>) new DrbdVlmData<>(
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
                            (DrbdVlmData<RSC>) new DrbdVlmData<>(
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
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("ApiContext does not have enough privileges");
        }
    }

    @Override
    public void create(DrbdRscData<?> drbdRscData) throws DatabaseException
    {
        errorReporter.logTrace("Creating DrbdRscData %s", getId(drbdRscData));
        update(drbdRscData);
    }

    private void update(DrbdRscData<?> drbdRscData)
    {
        try
        {
            K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
            tx.update(
                GeneratedDatabaseTables.LAYER_DRBD_RESOURCES,
                GenCrdCurrent.createLayerDrbdResources(
                    drbdRscData.getRscLayerId(),
                    drbdRscData.getPeerSlots(),
                    drbdRscData.getAlStripes(),
                    drbdRscData.getAlStripeSize(),
                    drbdRscData.getFlags().getFlagsBits(dbCtx),
                    drbdRscData.getNodeId().value
                )
            );
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
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.delete(
            GeneratedDatabaseTables.LAYER_DRBD_RESOURCES,
            GenCrdCurrent.createLayerDrbdResources(
                drbdRscData.getRscLayerId(),
                0,
                0,
                0,
                0,
                0
            )
        );
    }

    @Override
    public StateFlagsPersistence<DrbdRscData<?>> getRscStateFlagPersistence()
    {
        return rscFlagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<DrbdRscData<?>, NodeId> getNodeIdDriver()
    {
        return rscNodeIdDriver;
    }

    @Override
    public void persist(DrbdRscDfnData<?> drbdRscDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Creating DrbdRscDfnData %s", getId(drbdRscDfnData));
        update(drbdRscDfnData);
    }

    private void update(DrbdRscDfnData<?> drbdRscDfnData)
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.update(
            GeneratedDatabaseTables.LAYER_DRBD_RESOURCE_DEFINITIONS,
            GenCrdCurrent.createLayerDrbdResourceDefinitions(
                drbdRscDfnData.getResourceName().value,
                drbdRscDfnData.getRscNameSuffix(),
                drbdRscDfnData.getSnapshotName() == null ? null : drbdRscDfnData.getSnapshotName().value,
                drbdRscDfnData.getPeerSlots(),
                drbdRscDfnData.getAlStripes(),
                drbdRscDfnData.getAlStripeSize(),
                drbdRscDfnData.getTcpPort() == null ? null : drbdRscDfnData.getTcpPort().value,
                drbdRscDfnData.getTransportType().name(),
                drbdRscDfnData.getSecret() == null ? null : drbdRscDfnData.getSecret()
            )
        );
    }

    @Override
    public void delete(DrbdRscDfnData<?> drbdRscDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Deleting DrbdRscDfnData %s", getId(drbdRscDfnData));
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.delete(
            GeneratedDatabaseTables.LAYER_DRBD_RESOURCE_DEFINITIONS,
            GenCrdCurrent.createLayerDrbdResourceDefinitions(
                drbdRscDfnData.getResourceName().value,
                drbdRscDfnData.getRscNameSuffix(),
                drbdRscDfnData.getSnapshotName() == null ? null : drbdRscDfnData.getSnapshotName().value,
                0,
                0,
                0,
                null,
                null,
                null
            )
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TcpPortNumber> getTcpPortDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TcpPortNumber>) rscDfnUpdateDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TransportType> getTransportTypeDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnData<?>, TransportType>) rscDfnUpdateDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, String> getRscDfnSecretDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnData<?>, String>) rscDfnUpdateDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdRscDfnData<?>, Short> getPeerSlotsDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdRscDfnData<?>, Short>) rscDfnUpdateDriver;
    }

    @Override
    public void persist(DrbdVlmData<?> drbdVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating DrbdVlmData %s", getId(drbdVlmDataRef));
        update(drbdVlmDataRef);
    }

    private void update(DrbdVlmData<?> drbdVlmDataRef)
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        StorPool extStorPool = drbdVlmDataRef.getExternalMetaDataStorPool();
        String nodeNameStr = null;
        String poolNameStr = null;
        if (extStorPool != null)
        {
            nodeNameStr = extStorPool.getNode().getName().value;
            poolNameStr = extStorPool.getName().value;
        }
        tx.update(
            GeneratedDatabaseTables.LAYER_DRBD_VOLUMES,
            GenCrdCurrent.createLayerDrbdVolumes(
                drbdVlmDataRef.getRscLayerId(),
                drbdVlmDataRef.getVlmNr().value,
                nodeNameStr,
                poolNameStr
            )
        );
    }

    @Override
    public void delete(DrbdVlmData<?> drbdVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting DrbdVlmData %s", getId(drbdVlmDataRef));
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.delete(
            GeneratedDatabaseTables.LAYER_DRBD_VOLUMES,
            GenCrdCurrent.createLayerDrbdVolumes(
                drbdVlmDataRef.getRscLayerId(),
                drbdVlmDataRef.getVlmNr().value,
                null,
                null
            )
        );
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
        update(drbdVlmDfnDataRef);
    }

    private void update(DrbdVlmDfnData<?> drbdVlmDfnDataRef)
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.update(
            GeneratedDatabaseTables.LAYER_DRBD_VOLUME_DEFINITIONS,
            GenCrdCurrent.createLayerDrbdVolumeDefinitions(
                drbdVlmDfnDataRef.getResourceName().value,
                drbdVlmDfnDataRef.getRscNameSuffix(),
                drbdVlmDfnDataRef.getSnapshotName() == null ? null : drbdVlmDfnDataRef.getSnapshotName().value,
                drbdVlmDfnDataRef.getVolumeNumber().value,
                drbdVlmDfnDataRef.getMinorNr() == null ? null : drbdVlmDfnDataRef.getMinorNr().value
            )
        );
    }

    @Override
    public void delete(DrbdVlmDfnData<?> drbdVlmDfnDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting DrbdVlmDfnData %s", getId(drbdVlmDfnDataRef));
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.delete(
            GeneratedDatabaseTables.LAYER_DRBD_VOLUME_DEFINITIONS,
            GenCrdCurrent.createLayerDrbdVolumeDefinitions(
                drbdVlmDfnDataRef.getResourceName().value,
                drbdVlmDfnDataRef.getRscNameSuffix(),
                drbdVlmDfnDataRef.getSnapshotName() == null ? null : drbdVlmDfnDataRef.getSnapshotName().value,
                drbdVlmDfnDataRef.getVolumeNumber().value,
                null
            )
        );
    }
}
