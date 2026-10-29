package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.SharedResourceManager;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.internal.helpers.AtomicUpdateSatelliteData;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDefinitionUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotControllerFactory;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinitionControllerFactory;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeControllerFactory;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinitionControllerFactory;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.utils.SuspendLayerUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotVlmDfnDescriptionInline;
import static com.linbit.linstor.utils.layer.LayerVlmUtils.getStorPoolMap;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlSnapshotCrtHelper
{
    /** Layers a CBT snapshot may have, see {@link #ensureCbtSupported(Resource)} */
    private static final Set<DeviceLayerKind> CBT_LAYERS = EnumSet.of(
        DeviceLayerKind.DRBD,
        DeviceLayerKind.LUKS,
        DeviceLayerKind.CACHE,
        DeviceLayerKind.WRITECACHE,
        DeviceLayerKind.STORAGE
    );
    /** Storage providers a CBT snapshot may have, see {@link #ensureCbtSupported(Resource)} */
    private static final Set<DeviceProviderKind> CBT_PROVIDERS = EnumSet.of(
        DeviceProviderKind.LVM,
        DeviceProviderKind.LVM_THIN,
        DeviceProviderKind.ZFS,
        DeviceProviderKind.ZFS_THIN
    );
    /** First DRBD kernel module with changed block tracking */
    private static final ExtToolsInfo.Version CBT_DRBD_KMOD_VERSION = new ExtToolsInfo.Version(9, 3, 5);
    /** First drbd-utils with the changed block tracking commands */
    private static final ExtToolsInfo.Version CBT_DRBD_UTILS_VERSION = new ExtToolsInfo.Version(9, 36, 0);

    private final CtrlSnapshotHelper ctrlSnapshotHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final SnapshotDefinitionControllerFactory snapshotDefinitionFactory;
    private final SnapshotVolumeDefinitionControllerFactory snapshotVolumeDefinitionControllerFactory;
    private final SnapshotControllerFactory snapshotFactory;
    private final SnapshotVolumeControllerFactory snapshotVolumeControllerFactory;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final SharedResourceManager sharedRscMgr;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ErrorReporter errorReporter;

    @Inject
    public CtrlSnapshotCrtHelper(
        CtrlSnapshotHelper ctrlSnapshotHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        SnapshotDefinitionControllerFactory snapshotDefinitionFactoryRef,
        SnapshotVolumeDefinitionControllerFactory snapshotVolumeDefinitionControllerFactoryRef,
        SnapshotControllerFactory snapshotFactoryRef,
        SnapshotVolumeControllerFactory snapshotVolumeControllerFactoryRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        SharedResourceManager sharedRscMgrRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ErrorReporter errorReporterRef
    )
    {
        ctrlSnapshotHelper = ctrlSnapshotHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        snapshotDefinitionFactory = snapshotDefinitionFactoryRef;
        snapshotVolumeDefinitionControllerFactory = snapshotVolumeDefinitionControllerFactoryRef;
        snapshotFactory = snapshotFactoryRef;
        snapshotVolumeControllerFactory = snapshotVolumeControllerFactoryRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        sharedRscMgr = sharedRscMgrRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        errorReporter = errorReporterRef;
    }

    /**
     * Returns whether the given flags of a snapshot create request ask for changed block tracking
     * ({@link SnapshotDefinition.Flags#CBT}). No other flag can be requested.
     *
     * @param flags {@code null} if the request has none (e.g. JSON {@code "flags": null})
     */
    public static boolean isCbtRequested(@Nullable List<String> flags)
    {
        boolean cbt = false;
        if (flags != null)
        {
            for (String flag : flags)
            {
                if (ApiConsts.FLAG_CBT.equalsIgnoreCase(flag))
                {
                    cbt = true;
                }
                else
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.entryBuilder(
                            ApiConsts.FAIL_INVLD_REQUEST,
                            "The flag '" + flag + "' cannot be requested when creating a snapshot."
                        )
                            .setCorrection("The only flag that can be requested is '" + ApiConsts.FLAG_CBT + "'.")
                            .setSkipErrorReport(true)
                            .build()
                    );
                }
            }
        }
        return cbt;
    }

    /**
     * Creates the snapshot definition and the snapshots of the given (or all diskful) resources, marked to suspend
     * I/O.
     *
     * @param cbtRef whether the snapshot is a changed block tracking snapshot ({@link SnapshotDefinition.Flags#CBT})
     */
    public SnapshotDefinition createSnapshots(
        Collection<String> nodeNameStrs,
        ResourceName rscName,
        SnapshotName snapshotName,
        Map<String, String> props,
        boolean cbtRef,
        ApiCallRcImpl responses
    )
    {
        final ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName);

        SnapshotDefinition snapshotDfn = createSnapshotDfnData(
            rscDfn,
            snapshotName,
            new SnapshotDefinition.Flags[] {}
        );
        ctrlPropsHelper.copy(
            ctrlPropsHelper.getProps(rscDfn),
            ctrlPropsHelper.getProps(snapshotDfn, true)
        );

        // apply given creation props
        ctrlPropsHelper.fillProperties(
            responses, LinStorObject.SNAP_DFN, props, ctrlPropsHelper.getProps(snapshotDfn, false),
            ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN);

        ensureSnapshotsViable(rscDfn);

        setInCreation(snapshotDfn);

        Iterator<VolumeDefinition> vlmDfnIterator = iterateVolumeDfn(rscDfn);
        List<SnapshotVolumeDefinition> snapshotVolumeDefinitions = new ArrayList<>();
        while (vlmDfnIterator.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIterator.next();

            SnapshotVolumeDefinition snapshotVlmDfn = createSnapshotVlmDfnData(snapshotDfn, vlmDfn);
            snapshotVolumeDefinitions.add(snapshotVlmDfn);

            ctrlPropsHelper.copy(
                ctrlPropsHelper.getProps(vlmDfn),
                ctrlPropsHelper.getProps(snapshotVlmDfn, true)
            );
        }

        boolean resourceFound = false;
        if (nodeNameStrs.isEmpty())
        {
            Iterator<Resource> rscIterator = ctrlSnapshotHelper.iterateResource(rscDfn);
            while (rscIterator.hasNext())
            {
                Resource rsc = rscIterator.next();

                if (!isDisklessPrivileged(rsc))
                {
                    if (isEvacuatingPrivileged(rsc))
                    {
                        warnNodeEvacuating(rscName.displayValue, responses, rsc.getNode().getName().displayValue);
                    }
                    else if (!isNodeOnline(rsc))
                    {
                        warnNodeOffline(rscName.displayValue, responses, rsc.getNode().getName().displayValue);
                    }
                    else if (sharedRscMgr.isInactiveShared(rsc))
                    {
                        // only the active copy takes the snapshot of the shared data, but every copy
                        // of a shared storage pool also holds the snapshot: register the objects here
                        // as well (see takeSnapshot, which skips inactive shared copies)
                        createSnapshotOnNode(snapshotDfn, snapshotVolumeDefinitions, rsc);
                    }
                    else
                    {
                        Snapshot snap = createSnapshotOnNode(snapshotDfn, snapshotVolumeDefinitions, rsc);
                        setNodeIds(rsc, snap);
                        resourceFound = true;
                    }
                }
            }
        }
        else
        {
            for (String nodeNameStr : nodeNameStrs)
            {
                Resource rsc = ctrlApiDataLoader.loadRsc(rscDfn, nodeNameStr);

                if (isDisklessPrivileged(rsc))
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED,
                            "Cannot create snapshot from diskless resource on node '" + nodeNameStr + "'"
                        )
                    );
                }
                if (isEvacuatingPrivileged(rsc))
                {
                    warnNodeEvacuating(rscName.displayValue, responses, nodeNameStr);
                }
                else
                {
                    Snapshot snap = createSnapshotOnNode(snapshotDfn, snapshotVolumeDefinitions, rsc);
                    setNodeIds(rsc, snap);
                    resourceFound = true;
                }
            }

            // every copy of a shared storage pool also holds the snapshot data: register the snapshot
            // objects on all copies sharing a storage pool with a snapshotted resource, even if their
            // nodes were not requested
            Iterator<Resource> sharedRscIterator = ctrlSnapshotHelper.iterateResource(rscDfn);
            while (sharedRscIterator.hasNext())
            {
                Resource rsc = sharedRscIterator.next();
                if (!isDisklessPrivileged(rsc) &&
                    snapshotDfn.getSnapshot(rsc.getNode().getName()) == null &&
                    !isEvacuatingPrivileged(rsc) &&
                    isNodeOnline(rsc) &&
                    sharedRscMgr.findSnapshotOnSharedSp(snapshotDfn, rsc) != null)
                {
                    createSnapshotOnNode(snapshotDfn, snapshotVolumeDefinitions, rsc);
                }
            }
        }

        if (!resourceFound)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_RSC,
                    "No resources found for snapshotting"
                )
            );
        }

        if (cbtRef)
        {
            for (Snapshot snap : snapshotDfn.getAllSnapshots())
            {
                ensureCbtSupported(ctrlApiDataLoader.loadRsc(rscDfn, snap.getNodeName().displayValue));
            }
            enableCbt(snapshotDfn);
        }

        Iterator<Resource> rscIterator = ctrlSnapshotHelper.iterateResource(rscDfn);
        while (rscIterator.hasNext())
        {
            Resource rsc = rscIterator.next();
            setSuspend(rsc, true);
        }

        return snapshotDfn;
    }

    private void warnNodeOffline(String rscNameStr, ApiCallRcImpl responses, String nodeNameStr)
    {
        responses.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_WARN,
                "Snapshot for resource '" + rscNameStr + "' will not be created on node '" + nodeNameStr +
                    "' because that node is currently offline."
            )
        );
    }

    private void warnNodeEvacuating(String rscNameStr, ApiCallRcImpl responses, String nodeNameStr)
    {
        responses.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_WARN,
                "Snapshot for resource '" + rscNameStr + "' will not be created on node '" + nodeNameStr +
                    "' because that node is currently evacuating."
            )
        );
    }

    private void setNodeIds(Resource rsc, Snapshot snap)
    {
        List<Integer> nodeIds = new ArrayList<>();
        try
        {
            Set<AbsRscLayerObject<Resource>> drbdLayers = LayerRscUtils
                .getRscDataByLayer(rsc.getLayerData(), DeviceLayerKind.DRBD);

            if (drbdLayers.size() > 1)
            {
                throw new ImplementationError("Only one instance of DRBD-layer supported");
            }

            for (AbsRscLayerObject<Resource> layer : drbdLayers)
            {
                DrbdRscData<Resource> drbdLayer = (DrbdRscData<Resource>) layer;
                boolean intMeta = false;
                for (DrbdVlmData<Resource> drbdVlm : drbdLayer.getVlmLayerObjects().values())
                {
                    if (!drbdVlm.isUsingExternalMetaData())
                    {
                        intMeta = true;
                    }
                }
                if (intMeta)
                {
                    for (DrbdRscData<Resource> rscData : drbdLayer.getRscDfnLayerObject().getDrbdRscDataList())
                    {
                        if (!rscData.isDiskless())
                        {
                            /*
                             * diskless nodes do reserve a node-id for themselves, but the peer-slot is not used in the
                             * metadata of diskfull peers
                             */
                            nodeIds.add(rscData.getNodeId().value);
                        }
                    }
                }
            }
            snap.getSnapshotDefinition()
                .getSnapDfnProps()
                .setProp(
                    InternalApiConsts.KEY_BACKUP_NODE_IDS_TO_RESET,
                    StringUtils.join(nodeIds, InternalApiConsts.KEY_BACKUP_NODE_ID_SEPERATOR),
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        catch (InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Snapshot createSnapshotOnNode(
        SnapshotDefinition snapshotDfn,
        Collection<SnapshotVolumeDefinition> snapshotVolumeDefinitions,
        Resource rsc
    )
    {
        Snapshot snapshot = createSnapshot(snapshotDfn, rsc);
        ctrlPropsHelper.copy(
            ctrlPropsHelper.getProps(rsc),
            ctrlPropsHelper.getProps(snapshot, true)
        );

        setSuspend(snapshot);

        for (SnapshotVolumeDefinition snapshotVolumeDefinition : snapshotVolumeDefinitions)
        {
            SnapshotVolume snapVlm = createSnapshotVolume(rsc, snapshot, snapshotVolumeDefinition);

            ctrlPropsHelper.copy(
                ctrlPropsHelper.getProps(rsc.getVolume(snapshotVolumeDefinition.getVolumeNumber())),
                ctrlPropsHelper.getProps(snapVlm, true)
            );
        }
        return snapshot;
    }

    /**
     * Ensures the given resource's node holds the per-node Snapshot objects of every usable snapshot
     * whose data lives on a shared storage pool the resource uses. The snapshot data exists once on
     * the shared pool, so every node holding a copy of the resource effectively also holds those
     * snapshots: this creates the missing objects so LINSTOR's view matches that. Snapshots on other
     * shared spaces or on nodes without shared storage pools are not touched - their data is not
     * accessible from this resource's pools. Noop for resources without shared storage pools.
     */
    public List<SnapshotDefinition> ensureSnapshotObjectsPresent(Resource rsc)
    {
        List<SnapshotDefinition> createdFor = new ArrayList<>();
        if (sharedRscMgr.isBackedBySharedStorPool(rsc))
        {
            NodeName nodeName = rsc.getNode().getName();
            for (SnapshotDefinition snapDfn : rsc.getResourceDefinition().getSnapshotDfns())
            {
                if (snapDfn.getSnapshot(nodeName) == null &&
                    snapDfn.getFlags().isSet(SnapshotDefinition.Flags.SUCCESSFUL) &&
                    snapDfn.getFlags().isUnset(SnapshotDefinition.Flags.DELETE))
                {
                    @Nullable Snapshot peerSnap = sharedRscMgr.findSnapshotOnSharedSp(snapDfn, rsc);
                    if (peerSnap != null)
                    {
                        createSnapshotObjectsOnly(snapDfn, rsc, peerSnap);
                        createdFor.add(snapDfn);
                    }
                }
            }
        }
        return createdFor;
    }

    /**
     * Satellite update for the snapshot objects created by {@link #ensureSnapshotObjectsPresent(Resource)}.
     * Without it the new node would only learn about the snapshots on its next full sync - and a restore
     * from such a snapshot would silently create an empty volume, since the satellite cannot find the
     * snapshot to restore from.
     */
    public Flux<ApiCallRc> updateSatellitesForNewSnapshotObjects(
        ResourceDefinition rscDfn,
        Collection<SnapshotDefinition> snapDfnsRef
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        if (!snapDfnsRef.isEmpty())
        {
            flux = ctrlSatelliteUpdateCaller.updateSatellites(
                new AtomicUpdateSatelliteData().add(rscDfn).addSnapDfns(snapDfnsRef),
                CtrlSatelliteUpdateCaller.notConnectedWarn()
            )
                .transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        updateResponses,
                        rscDfn.getName(),
                        "Registered snapshot(s) of {1} on {0}"
                    )
                );
        }
        return flux;
    }

    /**
     * Creates the per-node Snapshot (and snapshot volume) objects of the given snapshot-definition for
     * the given resource - metadata only: the snapshot data was created by another node of the shared
     * storage pool, so no storage operation results from this. Props and the creation timestamp are
     * mirrored from the given peer snapshot, which has to live on a shared storage pool the resource
     * uses (see {@link SharedResourceManager#findSnapshotOnSharedSp}).
     */
    private Snapshot createSnapshotObjectsOnly(SnapshotDefinition snapshotDfn, Resource rsc, Snapshot peerSnap)
    {
        Snapshot snapshot = createSnapshot(snapshotDfn, rsc);
        ctrlPropsHelper.copy(
            ctrlPropsHelper.getProps(peerSnap, true),
            ctrlPropsHelper.getProps(snapshot, true)
        );
        ctrlPropsHelper.copy(
            ctrlPropsHelper.getProps(peerSnap, false),
            ctrlPropsHelper.getProps(snapshot, false)
        );
        try
        {
            @Nullable Instant peerCreateTs = peerSnap.getCreateTimestamp().orElse(null);
            if (peerCreateTs != null)
            {
                snapshot.setCreateTimestamp(peerCreateTs);
            }
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }

        for (SnapshotVolumeDefinition snapVlmDfn : snapshotDfn.getAllSnapshotVolumeDefinitions())
        {
            SnapshotVolume snapVlm = createSnapshotVolume(rsc, snapshot, snapVlmDfn);

            @Nullable SnapshotVolume peerSnapVlm = peerSnap.getVolume(snapVlmDfn.getVolumeNumber());
            if (peerSnapVlm != null)
            {
                ctrlPropsHelper.copy(
                    ctrlPropsHelper.getProps(peerSnapVlm, true),
                    ctrlPropsHelper.getProps(snapVlm, true)
                );
                ctrlPropsHelper.copy(
                    ctrlPropsHelper.getProps(peerSnapVlm, false),
                    ctrlPropsHelper.getProps(snapVlm, false)
                );
            }
        }
        return snapshot;
    }

    private void ensureSnapshotsViable(ResourceDefinition rscDfn)
    {
        // the counterpart of CtrlRscLiveMigrateHelper#ensureSharedDualActiveSupported, which refuses
        // opening the dual-active window while snapshots exist
        ResourceDefinitionUtils.ensureSharedDataNotActiveOnMultipleNodes(
            rscDfn,
            "Resource definition '" + rscDfn.getName() + "'",
            "snapshotted"
        );
        Iterator<Resource> rscIterator = ctrlSnapshotHelper.iterateResource(rscDfn);
        // ctrl, node, sp, rg, rd, r
        int diskFullConnected = 0;
        while (rscIterator.hasNext())
        {
            Resource currentRsc = rscIterator.next();
            Set<AbsRscLayerObject<Resource>> drbdLayerDataSet = LayerRscUtils.getRscDataByLayer(
                currentRsc.getLayerData(),
                DeviceLayerKind.DRBD
            );
            ReadOnlyProps stltProps = ctrlPropsHelper.getStltPropsForView();
            for (AbsRscLayerObject<Resource> drbdLayerData : drbdLayerDataSet)
            {
                DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) drbdLayerData;
                if (drbdRscData.isSkipDiskEnabled(stltProps))
                {
                    throw new ApiRcException(ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.FAIL_SNAPSHOT_NOT_UPTODATE,
                            "SkipDisk is enabled for resource " + rscDfn.getName()
                        )
                        .setDetails(
                            "Snapshots are not allowed while SkipDisk is enabled, " +
                                "because upToDate-state cannot be ensured."
                        )
                        .build()
                    );
                }
            }
            ensureDriversSupportSnapshots(currentRsc);
            if (!isDisklessPrivileged(currentRsc) && ctrlSnapshotHelper.satelliteConnected(currentRsc))
            {
                diskFullConnected++;
            }
        }

        if (diskFullConnected == 0)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_NOT_CONNECTED,
                    "No diskful connected satellite for snapshot or no resources."
                )
                .setDetails("Snapshots need at least one diskful online satellite.")
                .build()
            );
        }
    }

    private void ensureDriversSupportSnapshots(Resource rsc)
    {
        if (!isDisklessPrivileged(rsc))
        {
            Iterator<Volume> vlmIterator = rsc.iterateVolumes();
            while (vlmIterator.hasNext())
            {
                Volume vlm = vlmIterator.next();
                Map<String, StorPool> storPoolMap = getStorPoolMap(
                    vlm
                );

                for (StorPool storPool : storPoolMap.values())
                {
                    DeviceProviderKind providerKind = storPool.getDeviceProviderKind();
                    boolean supportsSnapshot = storPool.isSnapshotSupported();

                    if (!supportsSnapshot)
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.entryBuilder(
                                ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED,
                                "Storage driver '" + providerKind + "' " + "does not support snapshots."
                            ).setDetails(
                                "Used for storage pool '" + storPool.getName() + "'" +
                                    " on '" + rsc.getNode().getName() + "'."
                            ).build()
                        );
                    }
                }
            }
        }
    }

    /**
     * Changed block tracking needs DRBD, whose metadata records the changes, and a stack below DRBD that a CBT session
     * can open read-only: LUKS, CACHE or WRITECACHE layers and LVM, LVM-thin, ZFS or ZFS-thin storage for the data and
     * the external DRBD metadata. The node needs a DRBD kernel module and drbd-utils with changed block tracking.
     */
    private void ensureCbtSupported(Resource rsc)
    {
        String rscDescr = "resource '" + rsc.getResourceDefinition().getName().displayValue + "' on node '" +
            rsc.getNode().getName().displayValue + "'";
        List<DeviceLayerKind> layerStack = LayerRscUtils.getLayerStack(rsc);
        if (!layerStack.contains(DeviceLayerKind.DRBD))
        {
            throw cbtNotSupported(rscDescr, "Changed block tracking needs the DRBD layer.");
        }
        for (DeviceLayerKind layerKind : layerStack)
        {
            if (!CBT_LAYERS.contains(layerKind))
            {
                throw cbtNotSupported(rscDescr, "The layer " + layerKind + " is not supported.");
            }
        }
        Set<AbsRscLayerObject<Resource>> storRscDataSet = LayerRscUtils.getRscDataByLayer(
            rsc.getLayerData(),
            DeviceLayerKind.STORAGE,
            suffix -> suffix.equals(RscLayerSuffixes.SUFFIX_DATA) || suffix.equals(RscLayerSuffixes.SUFFIX_DRBD_META)
        );
        for (AbsRscLayerObject<Resource> storRscData : storRscDataSet)
        {
            for (VlmProviderObject<Resource> storVlmData : storRscData.getVlmLayerObjects().values())
            {
                DeviceProviderKind providerKind = storVlmData.getProviderKind();
                if (!CBT_PROVIDERS.contains(providerKind))
                {
                    throw cbtNotSupported(
                        rscDescr,
                        "The storage provider " + providerKind + " (storage pool '" +
                            storVlmData.getStorPool().getName().displayValue + "') is not supported."
                    );
                }
            }
        }
        ExtToolsManager extToolsMgr = rsc.getNode().getPeer().getExtToolsManager();
        ensureCbtVersion(rscDescr, extToolsMgr, ExtTools.DRBD9_KERNEL, "DRBD kernel module", CBT_DRBD_KMOD_VERSION);
        ensureCbtVersion(rscDescr, extToolsMgr, ExtTools.DRBD9_UTILS, "drbd-utils", CBT_DRBD_UTILS_VERSION);
    }

    private static void ensureCbtVersion(
        String rscDescr,
        ExtToolsManager extToolsMgr,
        ExtTools extTool,
        String toolDescr,
        ExtToolsInfo.Version minVersion
    )
    {
        @Nullable ExtToolsInfo info = extToolsMgr.getExtToolInfo(extTool);
        // unknown version parts count as "greater or equal", so an unsupported tool needs its own check
        if (info == null || !info.isSupported())
        {
            throw cbtNotSupported(rscDescr, "The node reports no supported " + toolDescr + ".");
        }
        if (!info.hasVersionOrHigher(minVersion))
        {
            throw cbtNotSupported(
                rscDescr,
                "The node has " + toolDescr + " " + info.getVersion() + ", but changed block tracking needs " +
                    minVersion + " or newer."
            );
        }
    }

    private static ApiRcException cbtNotSupported(String rscDescr, String cause)
    {
        return new ApiRcException(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.FAIL_CBT_NOT_SUPPORTED,
                "Changed block tracking is not supported for " + rscDescr + "."
            )
                .setCause(cause)
                .setDetails(
                    "Changed block tracking needs DRBD " + CBT_DRBD_KMOD_VERSION + " and drbd-utils " +
                        CBT_DRBD_UTILS_VERSION + " or newer, on LVM, LVM-thin, ZFS or ZFS-thin storage, " +
                        "optionally with LUKS, CACHE or WRITECACHE layers."
                )
                .setCorrection("Create the snapshot without the " + ApiConsts.FLAG_CBT + " flag.")
                .setSkipErrorReport(true)
                .build()
        );
    }

    /**
     * Marks the snapshot as CBT snapshot and generates its bookmark id: a random 64-bit number, formatted as DRBD
     * prints it (16 upper case hex digits). The satellites start the new tracking interval with this id.
     */
    private void enableCbt(SnapshotDefinition snapshotDfn)
    {
        long cbtId;
        do
        {
            cbtId = ThreadLocalRandom.current().nextLong();
        }
        while (cbtId == 0);
        try
        {
            snapshotDfn.getFlags().enableFlags(SnapshotDefinition.Flags.CBT);
            ctrlPropsHelper.getProps(snapshotDfn, false).setProp(
                ApiConsts.KEY_CBT_ID,
                String.format("%016X", cbtId),
                ApiConsts.NAMESPC_CBT
            );
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private boolean isDisklessPrivileged(Resource rsc)
    {
        boolean isDiskless;
        StateFlags<Flags> stateFlags = rsc.getStateFlags();
        isDiskless = stateFlags.isSomeSet(
            Resource.Flags.DRBD_DISKLESS,
            Resource.Flags.NVME_INITIATOR,
            Resource.Flags.EBS_INITIATOR
        );
        return isDiskless;
    }

    /**
     * Returns the resources that have to be activated before a snapshot of the given resource-definition
     * can be created: one for every shared storage pool backing the resource-definition where currently no
     * copy is active. The chosen resource must be activatable and its satellite online.
     *
     * @param nodeNameStrs if not empty, resources on these nodes are preferred, since the snapshot
     *     was requested there
     *
     * @throws ApiRcException {@link ApiConsts#FAIL_ONLY_ONE_ACT_RSC_PER_SHARED_STOR_POOL_ALLOWED} if all
     *     copies of a shared storage pool are inactive but none of them can be activated
     */
    public List<Resource> findSharedRscsToActivate(ResourceName rscName, Collection<String> nodeNameStrs)
    {
        final ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName);
        Set<NodeName> requestedNodes = new HashSet<>();
        for (String nodeNameStr : nodeNameStrs)
        {
            requestedNodes.add(LinstorParsingUtils.asNodeName(nodeNameStr));
        }

        List<Resource> ret = new ArrayList<>();
        Set<Resource> visited = new HashSet<>();
        Iterator<Resource> rscIterator = rscDfn.iterateResource();
        while (rscIterator.hasNext())
        {
            Resource rsc = rscIterator.next();
            if (
                !visited.contains(rsc) && !isDisklessPrivileged(rsc) &&
                    sharedRscMgr.isBackedBySharedStorPool(rsc)
            )
            {
                TreeSet<Resource> sharedGroup = sharedRscMgr.getSharedResources(rsc);
                sharedGroup.add(rsc);
                visited.addAll(sharedGroup);

                boolean anyActive = false;
                for (Resource sharedRsc : sharedGroup)
                {
                    if (
                        !sharedRsc.getStateFlags().isSomeSet(
                            Resource.Flags.INACTIVE,
                            Resource.Flags.INACTIVE_PERMANENTLY
                        )
                    )
                    {
                        anyActive = true;
                        break;
                    }
                }
                if (!anyActive)
                {
                    ret.add(findRscToActivate(sharedGroup, requestedNodes, rscName));
                }
            }
        }
        return ret;
    }

    /**
     * Picks the resource of an all-inactive shared group that should be activated, preferring resources
     * on the given requested nodes.
     *
     * @throws ApiRcException {@link ApiConsts#FAIL_ONLY_ONE_ACT_RSC_PER_SHARED_STOR_POOL_ALLOWED} if none
     *     of the resources can be activated
     */
    private Resource findRscToActivate(
        TreeSet<Resource> sharedGroup,
        Set<NodeName> requestedNodes,
        ResourceName rscName
    )
    {
        @Nullable Resource candidate = null;
        for (Resource sharedRsc : sharedGroup)
        {
            if (
                !sharedRsc.getStateFlags().isSet(Resource.Flags.INACTIVE_PERMANENTLY) &&
                    !isDisklessPrivileged(sharedRsc) &&
                    !isEvacuatingPrivileged(sharedRsc) &&
                    isNodeOnline(sharedRsc)
            )
            {
                boolean requested = requestedNodes.isEmpty() ||
                    requestedNodes.contains(sharedRsc.getNode().getName());
                if (requested)
                {
                    candidate = sharedRsc;
                    break;
                }
                if (candidate == null)
                {
                    candidate = sharedRsc;
                }
            }
        }
        if (candidate == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_ONLY_ONE_ACT_RSC_PER_SHARED_STOR_POOL_ALLOWED,
                    "Cannot create snapshot of resource '" + rscName.displayValue +
                        "' since all of its resources in a shared storage pool are inactive " +
                        "and none of them can be activated"
                )
                    .setCause(
                        "Snapshotting shared data requires an active resource, but no inactive copy " +
                            "is activatable: the node has to be online and not evacuating."
                    )
                    .setCorrection("Activate one of the resources first.")
                    .setSkipErrorReport(true)
                    .build()
            );
        }
        return candidate;
    }

    private boolean isNodeOnline(Resource rsc)
    {
        return ctrlSnapshotHelper.satelliteConnected(rsc);
    }

    private boolean isEvacuatingPrivileged(Resource rsc)
    {
        boolean isEvacuating;
        isEvacuating = rsc.getNode().getFlags().isSet(Node.Flags.EVACUATE);
        return isEvacuating;
    }

    public SnapshotDefinition createSnapshotDfnData(
        ResourceDefinition rscDfn,
        SnapshotName snapshotName,
        SnapshotDefinition.Flags[] snapshotDfnInitFlags
    )
    {
        SnapshotDefinition snapshotDfn;
        try
        {
            snapshotDfn = snapshotDefinitionFactory.create(
                rscDfn,
                snapshotName,
                snapshotDfnInitFlags
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN,
                    String.format(
                        "A snapshot definition with the name '%s' already exists in resource definition '%s'.",
                        snapshotName,
                        rscDfn.getName().displayValue
                    ),
                    true
                ),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return snapshotDfn;
    }

    public SnapshotVolumeDefinition createSnapshotVlmDfnData(SnapshotDefinition snapshotDfn, VolumeDefinition vlmDfn)
    {
        String descriptionInline = getSnapshotVlmDfnDescriptionInline(
            snapshotDfn.getResourceName().displayValue,
            snapshotDfn.getName().displayValue,
            vlmDfn.getVolumeNumber().value
        );
        long volumeSize = getVolumeSize(vlmDfn);

        @Nullable SnapshotVolumeDefinition snapshotVlmDfn;
        try
        {
            snapshotVlmDfn = snapshotDfn.getSnapshotVolumeDefinition(
                new VolumeNumber(vlmDfn.getVolumeNumber().value)
            );
            if (snapshotVlmDfn == null)
            {
                snapshotVlmDfn = snapshotVolumeDefinitionControllerFactory.create(
                    snapshotDfn,
                    vlmDfn,
                    volumeSize,
                    new SnapshotVolumeDefinition.Flags[] {}
                );
            }
            else
            {
                if (snapshotVlmDfn.getVolumeSize() != volumeSize)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_BACKUP_CONFIG,
                            "The snapshot-volume-definition " + descriptionInline +
                                " already exists but has a different size. Current size: " + snapshotVlmDfn
                                    .getVolumeSize() + ", expected size: " + volumeSize
                        )
                    );
                }
            }
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN,
                    String.format(
                        "Volume %d of snapshot definition with the name '%s' already exists in " +
                            "resource definition '%s'.",
                        vlmDfn.getVolumeNumber().value,
                        snapshotDfn.getName().displayValue,
                        snapshotDfn.getResourceName().displayValue
                    )
                ),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        catch (MdException mdExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_VLM_SIZE,
                    String.format(
                        "The %s has an invalid size of '%d'. Valid sizes range from %d to %d.",
                        descriptionInline,
                        volumeSize,
                        MetaData.DRBD_MIN_NET_kiB,
                        MetaData.DRBD_MAX_kiB
                    )
                ),
                mdExc
            );
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new ImplementationError(exc);
        }
        return snapshotVlmDfn;
    }

    private Snapshot createSnapshot(SnapshotDefinition snapshotDfn, Resource rsc)
    {
        String snapshotNameStr = snapshotDfn.getName().displayValue;
        String rscNameStr = rsc.getResourceDefinition().getName().displayValue;
        String nodeNameStr = rsc.getNode().getName().displayValue;

        Snapshot snapshot;
        try
        {
            snapshot = snapshotFactory.create(
                rsc,
                snapshotDfn,
                new Snapshot.Flags[0]
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT,
                    String.format(
                        "A snapshot with the name '%s' of the resource '%s' on '%s' already exists.",
                        snapshotNameStr,
                        rscNameStr,
                        nodeNameStr
                    )
                ),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return snapshot;
    }

    public Snapshot restoreSnapshot(
        SnapshotDefinition snapshotDfn,
        Node node,
        RscLayerDataApi layerData,
        Map<String, String> renameStorPoolsMap,
        @Nullable ApiCallRc apiCallRc
    )
    {
        String snapshotNameStr = snapshotDfn.getName().displayValue;
        String rscNameStr = snapshotDfn.getResourceName().displayValue;
        String nodeName = node.getName().displayValue;

        Snapshot snapshot;
        try
        {
            snapshot = snapshotFactory.restore(
                layerData,
                node,
                snapshotDfn,
                new Snapshot.Flags[0],
                renameStorPoolsMap,
                apiCallRc
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT,
                    String.format(
                        "A snapshot with the name '%s' of the resource '%s' on '%s' already exists.",
                        snapshotNameStr,
                        rscNameStr,
                        nodeName
                    )
                ),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return snapshot;
    }

    private SnapshotVolume createSnapshotVolume(
        Resource rsc,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition
    )
    {
        SnapshotVolume snapVlm;
        try
        {
            snapVlm = snapshotVolumeControllerFactory.create(
                rsc,
                snapshot,
                snapshotVolumeDefinition
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT,
                    String.format(
                        "Volume %d of snapshot '%s' of the resource '%s' on '%s' already exists.",
                        snapshotVolumeDefinition.getVolumeNumber().value,
                        snapshot.getSnapshotName(),
                        snapshot.getResourceName(),
                        snapshot.getNodeName()
                    )
                ),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return snapVlm;
    }

    public SnapshotVolume restoreSnapshotVolume(
        RscLayerDataApi layerData,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition,
        Map<String, String> renameStorPoolsMap,
        @Nullable ApiCallRc apiCallRc
    )
    {
        SnapshotVolume snapVlm;
        try
        {
            snapVlm = snapshotVolumeControllerFactory.restore(
                layerData,
                snapshot,
                snapshotVolumeDefinition,
                renameStorPoolsMap,
                apiCallRc
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT,
                    String.format(
                        "Volume %d of snapshot '%s' of the resource '%s' on '%s' already exists.",
                        snapshotVolumeDefinition.getVolumeNumber().value,
                        snapshot.getSnapshotName(),
                        snapshot.getResourceName(),
                        snapshot.getNodeName()
                    )
                ),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return snapVlm;
    }

    @Deprecated
    private void setSuspend(Snapshot snapshot)
    {
        snapshot.setSuspendResource(true);
    }

    private void setSuspend(Resource rsc, boolean suspend)
    {
        try
        {
            SuspendLayerUtils.setSuspend(rsc, suspend);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private long getVolumeSize(VolumeDefinition vlmDfn)
    {
        long volumeSize;
        volumeSize = vlmDfn.getVolumeSize();
        return volumeSize;
    }

    private Iterator<VolumeDefinition> iterateVolumeDfn(ResourceDefinition rscDfn)
    {
        Iterator<VolumeDefinition> vlmDfnIter;
        vlmDfnIter = rscDfn.iterateVolumeDfn();
        return vlmDfnIter;
    }

    private void setInCreation(SnapshotDefinition snapshotDfn)
    {
        try
        {
            snapshotDfn.setInCreation(true);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

}
