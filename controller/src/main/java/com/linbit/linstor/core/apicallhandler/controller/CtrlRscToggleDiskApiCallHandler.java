package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.CtrlStorPoolResolveHelper;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.BackgroundRunner;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.DiskfulBy;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.utils.MixedStorPoolHelper;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.EventWaiter;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.layer.resource.RscDrbdLayerHelper;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.tasks.AutoDiskfulTask;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.makeRscContext;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescription;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

/**
 * Adds disks to a diskless resource or removes disks to make a resource diskless.
 * <p>
 * When adding disks to a diskless resource, the states defined by the following flags are used:
 * <ol>
 *     <li>DISKLESS, DISK_ADD_REQUESTED - the transition has been requested but not yet started</li>
 *     <li>DISKLESS, DISK_ADD_REQUESTED, DISK_ADDING - the peers should prepare for the resource to gain disks</li>
 *     <li>none - the disks should be added</li>
 * </ol>
 * <p>
 * When removing disks to make a resource diskless, the states defined by the following flags are used:
 * <ol>
 *     <li>DISK_REMOVE_REQUESTED - the transition has been requested but not yet started</li>
 *     <li>DISKLESS, DISK_REMOVE_REQUESTED, DISK_REMOVING - the disks should be removed</li>
 *     <li>DISKLESS - the peers should acknowledge the removal of the disks</li>
 * </ol>
 */
@Singleton
public class CtrlRscToggleDiskApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final BackgroundRunner backgroundRunner;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlVlmCrtApiHelper ctrlVlmCrtApiHelper;
    private final CtrlStorPoolResolveHelper ctrlStorPoolResolveHelper;
    private final CtrlRscDeleteApiHelper ctrlRscDeleteApiHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlRscLayerDataFactory ctrlLayerStackHelper;
    private final RscDrbdLayerHelper ctrlDrbdLayerStackHelper;
    private final ResponseConverter responseConverter;
    private final ResourceStateEvent resourceStateEvent;
    private final EventWaiter eventWaiter;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final Provider<CtrlRscAutoHelper> rscAutoHelper;
    private final ErrorReporter errorReporter;
    private final CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandler;
    private final Provider<CtrlRscDfnApiCallHandler> ctrlRscDfnApiCallHandler;
    private final Provider<AutoDiskfulTask> autoDiskfulTaskProvider;
    private final MixedStorPoolHelper mixedStorPoolHelper;

    @Inject
    public CtrlRscToggleDiskApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        BackgroundRunner backgroundRunnerRef, CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlVlmCrtApiHelper ctrlVlmCrtApiHelperRef,
        CtrlStorPoolResolveHelper ctrlStorPoolResolveHelperRef,
        CtrlRscDeleteApiHelper ctrlRscDeleteApiHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlRscLayerDataFactory ctrlLayerStackHelperRef,
        RscDrbdLayerHelper ctrlDrbdLayerStackHelperRef,
        ResponseConverter responseConverterRef,
        ResourceStateEvent resourceStateEventRef,
        EventWaiter eventWaiterRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<CtrlRscAutoHelper> rscAutoHelperRef,
        ErrorReporter errorReporterRef,
        CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandlerRef,
        Provider<CtrlRscDfnApiCallHandler> ctrlRscDfnApiCallHandlerRef,
        Provider<AutoDiskfulTask> autoDiskfulTaskProviderRef,
        MixedStorPoolHelper mixedStorPoolHelperRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        backgroundRunner = backgroundRunnerRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlVlmCrtApiHelper = ctrlVlmCrtApiHelperRef;
        ctrlStorPoolResolveHelper = ctrlStorPoolResolveHelperRef;
        ctrlRscDeleteApiHelper = ctrlRscDeleteApiHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        ctrlLayerStackHelper = ctrlLayerStackHelperRef;
        ctrlDrbdLayerStackHelper = ctrlDrbdLayerStackHelperRef;
        responseConverter = responseConverterRef;
        resourceStateEvent = resourceStateEventRef;
        eventWaiter = eventWaiterRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        rscAutoHelper = rscAutoHelperRef;
        errorReporter = errorReporterRef;
        ctrlRscActivateApiCallHandler = ctrlRscActivateApiCallHandlerRef;
        ctrlRscDfnApiCallHandler = ctrlRscDfnApiCallHandlerRef;
        autoDiskfulTaskProvider = autoDiskfulTaskProviderRef;
        mixedStorPoolHelper = mixedStorPoolHelperRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
        throws AccessDeniedException
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        ResourceName rscName = rscDfn.getName();

        Iterator<Resource> rscIter = rscDfn.iterateResource(apiCtx);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            boolean diskAddRequested =
                rsc.getStateFlags().isSet(apiCtx, Resource.Flags.DISK_ADD_REQUESTED);
            boolean diskRemoveRequested =
                rsc.getStateFlags().isSet(apiCtx, Resource.Flags.DISK_REMOVE_REQUESTED);
            if (diskAddRequested || diskRemoveRequested)
            {
                NodeName nodeName = rsc.getNode().getName();
                fluxes.add(updateAndAdjustDisk(nodeName, rscName, diskRemoveRequested, false, context));
            }
        }

        return fluxes;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceConnected(Resource rsc)
        throws AccessDeniedException
    {
        ResponseContext context = makeRscContext(
            ApiOperation.makeModifyOperation(),
            rsc.getNode().getName().displayValue,
            rsc.getResourceDefinition().getName().displayValue
        );
        String migrateFromNodeNameStr = getPropsPrivileged(rsc).map().get(ApiConsts.KEY_RSC_MIGRATE_FROM);

        // Only restart the migration watch if adding the disk is complete
        boolean diskAddRequested = rsc.getStateFlags().isSet(apiCtx, Resource.Flags.DISK_ADD_REQUESTED);

        return migrateFromNodeNameStr == null || !diskAddRequested ?
            Collections.emptySet() :
            Collections.singleton(Flux.from(waitForMigration(
                context,
                rsc.getNode().getName(),
                rsc.getResourceDefinition().getName(),
                ctrlApiDataLoader.loadNode(migrateFromNodeNameStr, true).getName()
            )));
    }

    public Flux<ApiCallRc> resourceToggleDisk(
        String nodeNameStr,
        String rscNameStr,
        @Nullable String storPoolNameStr,
        @Nullable String migrateFromNodeNameStr,
        @Nullable List<String> layerListRef,
        boolean removeDisk,
        @Nullable Resource.DiskfulBy diskfulByRef
    )
    {
        return resourceToggleDisk(
            nodeNameStr,
            rscNameStr,
            storPoolNameStr,
            migrateFromNodeNameStr,
            layerListRef,
            removeDisk,
            diskfulByRef,
            false
        );
    }

    public Flux<ApiCallRc> resourceToggleDisk(
        String nodeNameStr,
        String rscNameStr,
        @Nullable String storPoolNameStr,
        @Nullable String migrateFromNodeNameStr,
        @Nullable List<String> layerListRef,
        boolean removeDisk,
        @Nullable Resource.DiskfulBy diskfulByRef,
        boolean toggleIntoTiebreakerRef
    )
    {
        ResponseContext context = makeRscContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Toggle disk",
                createLockGuard(),
                () -> toggleDiskInTransaction(
                    nodeNameStr,
                    rscNameStr,
                    storPoolNameStr,
                    migrateFromNodeNameStr,
                    layerListRef,
                    removeDisk,
                    diskfulByRef,
                    toggleIntoTiebreakerRef,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> toggleDiskInTransaction(
        String nodeNameStr,
        String rscNameStr,
        @Nullable String storPoolNameStr,
        @Nullable String migrateFromNodeNameStr,
        @Nullable List<String> layerListStr,
        boolean removeDisk,
        @Nullable Resource.DiskfulBy diskfulByRef,
        boolean toggleIntoTiebreakerRef,
        ResponseContext context
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);

        errorReporter.logInfo(
            "Toggle Disk on %s/%s %s", nodeNameStr, rscNameStr, removeDisk ? "removing disk" : "adding disk");

        Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, true);

        if (hasDiskAddRequested(rsc))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_RSC_BUSY,
                "Addition of disk to resource already requested",
                true
            ));
        }
        if (hasDiskRemoveRequested(rsc))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_RSC_BUSY,
                "Removal of disk from resource already requested",
                true
            ));
        }

        if (!removeDisk && !ctrlVlmCrtApiHelper.isDiskless(rsc))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_RSC_ALREADY_HAS_DISK,
                "Resource already has disk",
                true
            ));
        }
        if (removeDisk && ctrlVlmCrtApiHelper.isDiskless(rsc))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_RSC_ALREADY_DISKLESS,
                "Resource already diskless",
                true
            ));
        }

        ResourceDefinition rscDfn = rsc.getResourceDefinition();
        AccessContext peerCtx = peerAccCtx.get();
        if (removeDisk)
        {
            // Prevent removal of the last disk
            int haveDiskCount = countDisksAndIsOnline(rscDfn);
            if (haveDiskCount <= 1)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INSUFFICIENT_REPLICA_COUNT,
                    "Cannot remove the disk from the only online resource with a disk",
                    true
                ));
            }

            if (!LayerUtils.hasLayer(getLayerData(peerCtx, rsc), DeviceLayerKind.DRBD))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_LAYER_STACK,
                    "Toggle disk is only supported in combination with DRBD",
                    true
                ));
            }
        }
        else
        {
            ensureAllPeersHavePeerSlotLeft(rscDfn);
        }

        // Save the requested storage pool in the resource properties.
        // This does not cause the storage pool to be used automatically.
        Props rscProps = ctrlPropsHelper.getProps(rsc);
        if (storPoolNameStr == null || storPoolNameStr.isEmpty())
        {
            if (removeDisk)
            {
                rscProps.map().put(ApiConsts.KEY_STOR_POOL_NAME, LinStor.DISKLESS_STOR_POOL_NAME);
            }
            else
            {
                rscProps.map().remove(ApiConsts.KEY_STOR_POOL_NAME);
            }
        }
        else
        {
            ctrlPropsHelper.fillProperties(
                responses,
                LinStorObject.RSC,
                Collections.singletonMap(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr),
                rscProps,
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }

        /*
         * If any of the targeted storage pools is shared and already actively used by a shared resource,
         * we must set the INACTIVE flag for the current rsc.
         */
        boolean needsDeactivate = false;
        Flux<ApiCallRc> deactivateFlux = Flux.empty();

        LayerPayload payload = new LayerPayload();
        if (isSharedSourceStorPool(rsc))
        {
            payload.drbdRsc.needsNewNodeId = true;
        }

        // Resolve storage pool now so that nothing is committed if the storage pool configuration is invalid
        Set<StorPoolName> storPoolNames = new HashSet<>();
        Iterator<Volume> vlmIter = rsc.iterateVolumes();
        while (vlmIter.hasNext())
        {
            Volume vlm = vlmIter.next();
            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();

            StorPool sp = ctrlStorPoolResolveHelper
                .resolveStorPool(rsc, vlmDfn, removeDisk)
                .extractApiCallRc(responses);
            storPoolNames.add(sp.getName());
            if (isSharedSpAlreadyUsed(rsc, sp))
            {
                needsDeactivate = true;
                payload.drbdRsc.needsNewNodeId = true;
            }
        }
        if (storPoolNames.size() == 1)
        {
            /*
             * Since we also set the StorPoolName property when going diskless, we should also set it then going diskful
             * (and this property makes sense to be set on resource-level)
             */
            ctrlPropsHelper.fillProperties(
                responses,
                LinStorObject.RSC,
                Collections.singletonMap(ApiConsts.KEY_STOR_POOL_NAME, storPoolNames.iterator().next().displayValue),
                rscProps,
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }

        if (removeDisk)
        {
            // diskful -> diskless
            markDiskRemoveRequested(rsc);

            // before we can update the layerData we first have to remove the actual disk.
            // that means we can only update the storage layer if the deviceManager already got rid
            // of the actual volume(s)

            removeDiskfulByProp(rsc);
        }
        else
        {
            // diskless -> diskful
            // ctrlLayerStackHelper.resetStoragePools(rsc);

            markDiskAddRequested(rsc);

            if (migrateFromNodeNameStr != null && !migrateFromNodeNameStr.isEmpty())
            {
                Resource migrateFromRsc = ctrlApiDataLoader.loadRsc(migrateFromNodeNameStr, rscNameStr, true);

                ensureNoSnapshots(migrateFromRsc);

                setMigrateFrom(rsc, migrateFromRsc.getNode().getName());

                ctrlRscDeleteApiHelper.ensureNotInUse(migrateFromRsc);
            }

            // List<DeviceLayerKind> layerList = null;
            if (needsDeactivate)
            {
                responses.addEntries(
                    ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.WARN_RSC_DEACTIVATED,
                        "Resource got deactivated as target shared storage pool is already used by a shared resource"
                        )
                    );
                deactivateFlux = ctrlRscActivateApiCallHandler.deactivateRsc(
                    rsc.getNode().getName().displayValue,
                    rscDfn.getName().displayValue
                );

                /*
                 * We also have to remove the currently diskless DrbdRscData and free up the node-id as now we must
                 * use the shared resource's node-id
                 */
            }
            else
            {
                copyDrbdNodeIdIfExists(rsc, payload);
            }
            /*
             * rebuilds the layerdata in case we just removed it..
             * doing so, we need to rebuild the layerstack instead of taking the previous one, as a diskless resource
             * might be "DRBD,storage" while its diskfull peers actually have "DRBD,LUKS,storage".
             */
            removeLayerData(rsc);
            List<DeviceLayerKind> layerList = CtrlRscCrtApiHelper.getLayerstackOrBuildDefault(
                peerCtx,
                ctrlLayerStackHelper,
                errorReporter,
                layerListStr,
                responses,
                rscDfn
            );

            ctrlLayerStackHelper.ensureStackDataExists(rsc, layerList, payload);

            setDiskfulByProp(rsc, diskfulByRef);
        }

        try
        {
            List<DeviceLayerKind> unsupportedLayers = CtrlRscCrtApiHelper.getUnsupportedLayers(peerCtx, rsc);
            if (!unsupportedLayers.isEmpty())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_STLT_DOES_NOT_SUPPORT_LAYER,
                        "Satellite '" + rsc.getNode().getName() + "' does not support the following layers: " +
                            unsupportedLayers
                    )
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "toggle disk resource " + rscDfn.getName().displayValue + " on node " +
                    rsc.getNode().getName(),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }

        ctrlTransactionHelper.commit();

        String action = removeDisk ? "Removal of disk from" : "Addition of disk to";
        responses.addEntry(ApiCallRcImpl.simpleEntry(
            ApiConsts.MODIFIED,
            action + " resource '" + rscDfn.getName().displayValue + "' " +
                "on node '" + rsc.getNode().getName().displayValue + "' registered"
        ));

        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(deactivateFlux)
            .concatWith(updateAndAdjustDisk(nodeName, rscName, removeDisk, toggleIntoTiebreakerRef, context))
            .concatWith(ctrlRscDfnApiCallHandler.get().updateProps(rscDfn));
    }

    private void ensureAllPeersHavePeerSlotLeft(ResourceDefinition rscDfnRef)
    {
        try
        {
            AccessContext peerCtx = peerAccCtx.get();
            List<Resource> diskfulRscList = rscDfnRef.getDiskfulResources(peerCtx);

            /*
             * usually we need one peer slot less than we have diskful resources, but we are about to toggle disk a
             * resource so the "new" diskful resource count will be +1. Therefore the -1 and +1 eliminate each other.
             */
            int requiredPeerSlots = diskfulRscList.size();

            LinkedHashMap<Resource, Short> rscListWithInsufficientPeerSlots = new LinkedHashMap<>();
            for (Resource rsc : diskfulRscList)
            {
                Set<AbsRscLayerObject<Resource>> drbdRscDataSet = LayerRscUtils.getRscDataByLayer(
                    getLayerData(apiCtx, rsc),
                    DeviceLayerKind.DRBD
                );
                if (drbdRscDataSet.size() >= 2)
                {
                    throw new ImplementationError("Unexpected layer tree");
                }
                if (!drbdRscDataSet.isEmpty())
                {
                    DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) drbdRscDataSet.iterator().next();
                    if (drbdRscData.getPeerSlots() < requiredPeerSlots)
                    {
                        rscListWithInsufficientPeerSlots.put(rsc, drbdRscData.getPeerSlots());
                    }
                }
            }
            if (!rscListWithInsufficientPeerSlots.isEmpty())
            {
                StringBuilder detailsBuilder = new StringBuilder("Resources with insufficient peer slots (")
                    .append(requiredPeerSlots)
                    .append(" required): \n");
                for (Entry<Resource, Short> entry : rscListWithInsufficientPeerSlots.entrySet())
                {
                    detailsBuilder.append(" * ")
                        .append(entry.getKey().toString())
                        .append(" has peer slots: ")
                        .append(entry.getValue())
                        .append("\n");
                }
                detailsBuilder.setLength(detailsBuilder.length() - 1);

                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_INSUFFICIENT_PEER_SLOTS,
                        "Existing resources do not have enough peer slots"
                    )
                        .setDetails(detailsBuilder.toString())
                        .setAppendObjectDescriptionToDetails(false)
                        .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check rscDfn for resources with insufficient peer slots",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
    }

    /**
     * Although we need to rebuild the layerData as the layerList might have changed, if we do not
     * deactivate (i.e. down) the current resource, we need to make sure that deleting DrbdRscData
     * and recreating a new DrbdRscData ends up with the same node-id as before.
     */
    private void copyDrbdNodeIdIfExists(Resource rsc, LayerPayload payload) throws ImplementationError
    {
        Set<AbsRscLayerObject<Resource>> drbdRscDataSet = LayerRscUtils.getRscDataByLayer(
            getLayerData(apiCtx, rsc),
            DeviceLayerKind.DRBD
        );
        if (drbdRscDataSet.size() >= 2)
        {
            throw new ImplementationError("Unexpected layer tree");
        }
        if (!drbdRscDataSet.isEmpty())
        {
            DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) drbdRscDataSet.iterator().next();
            payload.drbdRsc.replacingOldLayerRscId = drbdRscData.getRscLayerId();
            payload.drbdRsc.nodeId = drbdRscData.getNodeId().value;
        }
    }

    private List<DeviceLayerKind> removeLayerData(Resource rscRef)
    {
        List<DeviceLayerKind> layerList;
        try
        {
            layerList = LayerRscUtils.getLayerStack(rscRef, apiCtx);
            AbsRscLayerObject<Resource> layerData = rscRef.getLayerData(apiCtx);
            layerData.delete(apiCtx);
            rscRef.setLayerData(apiCtx, null);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        return layerList;
    }

    private boolean isSharedSpAlreadyUsed(Resource rscRef, StorPool sp)
    {
        boolean sharedSPAlreadyInUse = false;
        try
        {
            if (sp.isShared())
            {
                SharedStorPoolName sharedStorPoolName = sp.getSharedStorPoolName();
                ResourceDefinition rscDfn = rscRef.getResourceDefinition();
                Iterator<Resource> rscIt = rscDfn.iterateResource(apiCtx);
                while (rscIt.hasNext())
                {
                    Resource otherRsc = rscIt.next();
                    if (!otherRsc.equals(rscRef))
                    {
                        Set<StorPool> otherStorPools = LayerVlmUtils.getStorPools(otherRsc, apiCtx);
                        for (StorPool otherStorPool : otherStorPools)
                        {
                            if (otherStorPool.getSharedStorPoolName().equals(sharedStorPoolName))
                            {
                                sharedSPAlreadyInUse = true;
                            }
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

        return sharedSPAlreadyInUse;
    }

    // Restart from here when connection established and flag set
    private Flux<ApiCallRc> updateAndAdjustDisk(
        NodeName nodeName,
        ResourceName rscName,
        boolean removeDisk,
        boolean toggleIntoTiebreakerRef,
        ResponseContext context
    )
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Update for disk toggle",
                createLockGuard(),
                () -> updateAndAdjustDiskInTransaction(nodeName, rscName, removeDisk, toggleIntoTiebreakerRef, context)
            );
    }

    private Flux<ApiCallRc> updateAndAdjustDiskInTransaction(
        NodeName nodeName,
        ResourceName rscName,
        boolean removeDisk,
        boolean toggleIntoTiebreakerRef,
        ResponseContext context
    )
    {
        Flux<ApiCallRc> responses;

        Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, true);

        ApiCallRcImpl offlineWarnings = new ApiCallRcImpl();

        try
        {
            Node node = rsc.getNode();
            if (node.getPeer(apiCtx).getConnectionStatus() != ApiConsts.ConnectionStatus.ONLINE)
            {
                offlineWarnings.addEntry(ResponseUtils.makeNotConnectedWarning(node.getName()));
            }
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }

        // Don't start the operation if any of the required nodes are offline
        if (!offlineWarnings.isEmpty())
        {
            responses = Flux.just(offlineWarnings);
        }
        else
        {
            if (removeDisk)
            {
                markDiskRemoving(rsc, toggleIntoTiebreakerRef);
            }
            else
            {
                markDiskAdding(rsc);
                handleMixedStoragePool(rsc);
            }

            ctrlTransactionHelper.commit();

            String actionSelf = removeDisk ? "Removed disk on {0}" : null;
            String actionPeer = removeDisk ? null : "Prepared {0} to expect disk on ''" + nodeName.displayValue + "''";
            Flux<ApiCallRc> nextStep = finishOperation(nodeName, rscName, removeDisk, context);
            Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(
                    rsc.getResourceDefinition(), CtrlSatelliteUpdateCaller.notConnectedIgnoreIfNot(nodeName), nextStep)
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    updateResponses,
                    rscName,
                    Collections.singleton(nodeName),
                    actionSelf,
                    actionPeer
                ));

            responses = satelliteUpdateResponses
                // If an update fails (e.g. the connection to a node is lost), attempt to reset back to the
                // initial state. The requested flag is not reset, so the operation will be retried when the
                // nodes are next all connected.
                // There is no point attempting to reset a disk removal because the underlying storage volume
                // may have been removed.
                .transform(flux -> removeDisk ? flux :
                    flux
                        .onErrorResume(error ->
                            resetDiskAdding(nodeName, rscName)
                                .concatWith(Flux.error(error))
                        )
                )
                .concatWith(nextStep)
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
        }

        return responses;
    }

    private void handleMixedStoragePool(Resource rscRef)
    {
        Iterator<Volume> vlmsIt = rscRef.iterateVolumes();
        try
        {
            while (vlmsIt.hasNext())
            {
                Volume vlm = vlmsIt.next();
                mixedStorPoolHelper.handleMixedStoragePools(vlm);
            }
        }
        catch (StorageException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An error occured while checking for mixed storage setups"
                ),
                exc
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "checking for mixed storage setups", ApiConsts.FAIL_ACC_DENIED_VLM);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private Flux<ApiCallRc> resetDiskAdding(NodeName nodeName, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Reset disk adding",
                createLockGuard(),
                () -> resetDiskAddingInTransaction(nodeName, rscName)
            );
    }

    private Flux<ApiCallRc> resetDiskAddingInTransaction(
        NodeName nodeName,
        ResourceName rscName
    )
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, true);

        unmarkDiskAdding(rsc);

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(rsc, Flux.empty())
            .transform(responses -> CtrlResponseUtils.combineResponses(
                errorReporter,
                responses,
                rscName,
                "Diskless state temporarily reset on {0}"
            ));

        return satelliteUpdateResponses
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
    }

    private Flux<ApiCallRc> finishOperation(
        NodeName nodeName,
        ResourceName rscName,
        boolean removeDisk,
        ResponseContext context
    )
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Finish disk toggle",
                createLockGuard(),
                () -> finishOperationInTransaction(nodeName, rscName, removeDisk, context)
            );
    }

    private Flux<ApiCallRc> finishOperationInTransaction(
        NodeName nodeName,
        ResourceName rscName,
        boolean removeDisk,
        ResponseContext context
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, true);

        List<DeviceLayerKind> layerList = null;
        LayerPayload payload = new LayerPayload();
        Flux<ApiCallRc> activateFlux = Flux.empty();
        if (removeDisk)
        {
            markDiskRemoved(rsc);

            activateFlux = activateIfPossible(rsc);
            /*
             * We also have to remove the possible meta-children of previous StorageRscData.
             * LayerData will be recreated with ensureStackDataExists.
             * However, we still need to remember our node-id if we had / have DRBD in the list
             */
            copyDrbdNodeIdIfExists(rsc, payload);
            layerList = removeLayerData(rsc);
        }
        else
        {
            markDiskAdded(rsc);
            ctrlLayerStackHelper.resetStoragePools(rsc);
        }
        ctrlLayerStackHelper.ensureStackDataExists(rsc, layerList, payload);

        Flux<ApiCallRc> autoFlux = rscAutoHelper.get().manage(
            new AutoHelperContext(responses, context, rsc.getResourceDefinition())
        ).getFlux();

        ctrlTransactionHelper.commit();

        autoDiskfulTaskProvider.get().update(rsc);

        String actionSelf = removeDisk ? null : "Added disk on {0}";
        String actionPeer = removeDisk ?
            "Notified {0} that disk has been removed on ''" + nodeName.displayValue + "''" : null;
        Publisher<ApiCallRc> migrationFlux;
        ReadOnlyProps rscProps = getPropsPrivileged(rsc);
        String migrateFromNodeNameStr = rscProps.map().get(ApiConsts.KEY_RSC_MIGRATE_FROM);
        if (migrateFromNodeNameStr == null)
        {
            migrationFlux = Flux.empty();
        }
        else
        {
            migrationFlux = waitForMigration(
                context,
                nodeName,
                rscName,
                ctrlApiDataLoader.loadNode(migrateFromNodeNameStr, true).getName()
            );
        }

        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(activateFlux)
            .concatWith(
                ctrlSatelliteUpdateCaller.updateSatellites(
                    rsc.getResourceDefinition(),
                    CtrlSatelliteUpdateCaller.notConnectedErrorForNodesWarnForOthers(nodeName),
                    migrationFlux
                )
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    updateResponses,
                    rscName,
                    Collections.singleton(nodeName),
                    actionSelf,
                    actionPeer
                )))
            .concatWith(migrationFlux)
            .concatWith(autoFlux);
    }

    private Flux<ApiCallRc> activateIfPossible(Resource rsc)
    {
        Flux<ApiCallRc> ret = Flux.empty();
        StateFlags<Flags> rscFlags = rsc.getStateFlags();
        try
        {
            if (
                rscFlags.isSet(apiCtx, Resource.Flags.INACTIVE) &&
                !rscFlags.isSet(apiCtx, Resource.Flags.INACTIVE_PERMANENTLY)
            )
            {
                ret = ctrlRscActivateApiCallHandler.activateRsc(
                    rsc.getNode().getName().displayValue,
                    rsc.getResourceDefinition().getName().displayValue
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    private boolean isSharedSourceStorPool(Resource rsc)
    {
        boolean ret = false;
        try
        {
            Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc, apiCtx);
            for (StorPool sp : storPools)
            {
                if (isSharedSpAlreadyUsed(rsc, sp))
                {
                    ret = true;
                    break;
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    Publisher<ApiCallRc> waitForMigration(
        ResponseContext contextRef,
        NodeName nodeName,
        ResourceName rscName,
        NodeName migrateFromNodeName
    )
    {
        Mono<Tuple2<ResourceState, ResourceState>> migratedAndNotInUse = Mono.zip(
            eventWaiter.waitForStream(
                    resourceStateEvent.get(),
                    ObjectIdentifier.resource(nodeName, rscName)
                )
                .skipUntil(usageState -> usageState.getUpToDate())
                .next(),
            eventWaiter.waitForStream(
                    resourceStateEvent.get(),
                    ObjectIdentifier.resource(migrateFromNodeName, rscName)
                )
                .skipUntil(usageState -> usageState.getInUse() != null && !usageState.getInUse())
                .next()
        );

        return Mono.fromRunnable(() -> backgroundRunner.runInBackground(
            "Migrate '" + rscName + "' from '" + migrateFromNodeName + "' to '" + nodeName + "'",
            migratedAndNotInUse
                .thenMany(scopeRunner.fluxInTransactionalScope(
                    "Delete after migrate",
                    lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.RSC_DFN_MAP),
                    () -> startDeletionInTransaction(contextRef, nodeName, rscName, migrateFromNodeName)
                ))
                .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty())
        ));
    }

    private Flux<ApiCallRc> startDeletionInTransaction(
        ResponseContext contextRef,
        NodeName nodeName,
        ResourceName rscName,
        NodeName migrateFromNodeName
    )
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, true);
        Resource migrateFromRsc = ctrlApiDataLoader.loadRsc(migrateFromNodeName, rscName, false);

        getPropsPrivileged(rsc).map().remove(ApiConsts.KEY_RSC_MIGRATE_FROM);

        Flux<ApiCallRc> deleteFlux;
        if (migrateFromRsc == null)
        {
            deleteFlux = Flux.empty();
        }
        else
        {
            ctrlRscDeleteApiHelper.markDeletedWithVolumes(migrateFromRsc);
            deleteFlux = ctrlRscDeleteApiHelper.updateSatellitesForResourceDelete(
                contextRef,
                Collections.singleton(migrateFromNodeName),
                rscName
            );
        }

        ctrlTransactionHelper.commit();

        return deleteFlux;
    }

    private int countDisksAndIsOnline(ResourceDefinition rscDfn)
    {
        int haveDiskCount = 0;
        try
        {
            Iterator<Resource> rscIter = rscDfn.iterateResource(peerAccCtx.get());
            while (rscIter.hasNext())
            {
                Resource rsc = rscIter.next();
                if (!ctrlVlmCrtApiHelper.isDiskless(rsc) &&
                    rsc.getNode().getPeer(peerAccCtx.get()).getConnectionStatus() == ApiConsts.ConnectionStatus.ONLINE)
                {
                    haveDiskCount++;
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "count disks in " + getRscDfnDescription(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return haveDiskCount;
    }

    private boolean hasDiskAddRequested(Resource rsc)
    {
        boolean set;
        try
        {
            set = rsc.getStateFlags().isSet(peerAccCtx.get(), Resource.Flags.DISK_ADD_REQUESTED);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check whether addition of disk requested for " + getRscDescription(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return set;
    }

    private boolean hasDiskRemoveRequested(Resource rsc)
    {
        boolean set;
        try
        {
            set = rsc.getStateFlags().isSet(peerAccCtx.get(), Resource.Flags.DISK_REMOVE_REQUESTED);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check whether removal of disk requested for " + getRscDescription(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return set;
    }

    private void markDiskAddRequested(Resource rsc)
    {
        try
        {
            rsc.getStateFlags().enableFlags(peerAccCtx.get(), Resource.Flags.DISK_ADD_REQUESTED);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getRscDescription(rsc) + " adding disk",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void ensureNoSnapshots(Resource rsc)
    {
        try
        {
            for (SnapshotDefinition snapshotDfn : rsc.getResourceDefinition().getSnapshotDfns(peerAccCtx.get()))
            {
                Snapshot snapshot = snapshotDfn.getSnapshot(peerAccCtx.get(), rsc.getNode().getName());
                if (snapshot != null)
                {
                    throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_EXISTS_SNAPSHOT,
                        "Cannot migrate '" + rsc.getResourceDefinition().getName() + "' " +
                            "from '" + rsc.getNode().getName() + "' because snapshots are present " +
                            "and snapshots cannot be migrated"
                    ));
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check for snapshots of " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
    }

    private void setMigrateFrom(Resource rsc, NodeName migrateFromNodeName)
    {
        try
        {
            rsc.getProps(peerAccCtx.get()).map().put(ApiConsts.KEY_RSC_MIGRATE_FROM, migrateFromNodeName.value);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "set migration source for " + getRscDescription(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
    }

    private void removeDiskfulByProp(Resource rscRef)
    {
        try
        {
            rscRef.getProps(peerAccCtx.get()).removeProp(ApiConsts.KEY_RSC_DISKFUL_BY);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "removing property 'DiskfulBy'",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private void setDiskfulByProp(Resource rscRef, @Nullable DiskfulBy diskfulByRef)
    {
        try
        {
            Props rscProp = rscRef.getProps(peerAccCtx.get());
            if (diskfulByRef != null)
            {
                rscProp.setProp(ApiConsts.KEY_RSC_DISKFUL_BY, diskfulByRef.getValue());
            }
            else
            {
                rscProp.removeProp(ApiConsts.KEY_RSC_DISKFUL_BY);
            }
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "removing property 'DiskfulBy'",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private void markDiskRemoveRequested(Resource rsc)
    {
        try
        {
            rsc.getStateFlags().enableFlags(peerAccCtx.get(), Resource.Flags.DISK_REMOVE_REQUESTED);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getRscDescription(rsc) + " adding disk",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void markDiskAdding(Resource rsc)
    {
        try
        {
            rsc.getStateFlags().enableFlags(apiCtx, Resource.Flags.DISK_ADDING);
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void markDiskRemoving(Resource rsc, boolean toggleIntoTiebreakerRef)
    {
        try
        {
            rsc.getStateFlags().enableFlags(
                apiCtx,
                toggleIntoTiebreakerRef ? Resource.Flags.TIE_BREAKER : Resource.Flags.DRBD_DISKLESS,
                Resource.Flags.DISK_REMOVING
            );
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void unmarkDiskAdding(Resource rsc)
    {
        try
        {
            rsc.getStateFlags().disableFlags(apiCtx, Resource.Flags.DISK_ADDING);
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void markDiskAdded(Resource rscData)
    {
        try
        {
            rscData.getStateFlags().disableFlags(
                apiCtx,
                Resource.Flags.DRBD_DISKLESS,
                Resource.Flags.DISK_ADDING,
                Resource.Flags.DISK_ADD_REQUESTED
            );
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void markDiskRemoved(Resource rscData)
    {
        try
        {
            rscData.getStateFlags().disableFlags(
                apiCtx,
                Resource.Flags.DISK_REMOVING,
                Resource.Flags.DISK_REMOVE_REQUESTED
            );
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Props getPropsPrivileged(Resource rsc)
    {
        Props props;
        try
        {
            props = rsc.getProps(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return props;
    }

    static AbsRscLayerObject<Resource> getLayerData(AccessContext accCtx, Resource rsc)
    {
        AbsRscLayerObject<Resource> layerData;
        try
        {
            layerData = rsc.getLayerData(accCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "access layer data",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return layerData;
    }

    private LockGuard createLockGuard()
    {
        return lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP);
    }
}
