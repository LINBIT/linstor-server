package com.linbit.linstor.core.devmgr;

import com.linbit.ImplementationError;
import com.linbit.Platform;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.EntryBuilder;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.StltExternalFileHandler;
import com.linbit.linstor.core.SysFsHandler;
import com.linbit.linstor.core.UdevHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.DeviceLayer.AbortLayerProcessingException;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.LayerFactory;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.layer.LayerSizeHelper;
import com.linbit.linstor.layer.storage.StorageLayer;
import com.linbit.linstor.layer.storage.utils.LsBlkUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.LsBlkEntry;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.utils.MkfsUtils;
import com.linbit.linstor.utils.SetUtils;
import com.linbit.utils.Either;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

@Singleton
public class DeviceHandlerImpl implements DeviceHandler
{

    private static final int LSBLK_DISC_GRAN_RETRY_COUNT = 10;
    private static final long LSBLK_DISC_GRAN_RETRY_TIMEOUT_IN_MS = 100;

    private final AccessContext wrkCtx;
    private final ErrorReporter errorReporter;
    private final Provider<NotificationListener> notificationListenerProvider;

    private final ControllerPeerConnector controllerPeerConnector;
    private final CtrlStltSerializer interComSerializer;
    private final ResourceStateEvent resourceStateEvent;

    private final LayerFactory layerFactory;
    private final AtomicBoolean fullSyncApplied;
    private final StorageLayer storageLayer;
    private final ExtCmdFactory extCmdFactory;
    private final SnapshotShippingService snapshotShippingManager;

    private final SysFsHandler sysFsHandler;
    private final UdevHandler udevHandler;
    private final StltExternalFileHandler extFileHandler;

    private Props localNodeProps;
    private final BackupShippingMgr backupShippingManager;
    private final SuspendManager suspendMgr;
    private final LayerSizeHelper layerSizeHelper;

    @Inject
    public DeviceHandlerImpl(
        @DeviceManagerContext AccessContext wrkCtxRef,
        ErrorReporter errorReporterRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer interComSerializerRef,
        Provider<NotificationListener> notificationListenerRef,
        LayerFactory layerFactoryRef,
        StorageLayer storageLayerRef,
        ResourceStateEvent resourceStateEventRef,
        ExtCmdFactory extCmdFactoryRef,
        SysFsHandler sysFsHandlerRef,
        UdevHandler udevHandlerRef,
        SnapshotShippingService snapshotShippingManagerRef,
        StltExternalFileHandler extFileHandlerRef,
        BackupShippingMgr backupShippingManagerRef,
        SuspendManager suspendMgrRef,
        LayerSizeHelper layerSizeHelperRef
    )
    {
        wrkCtx = wrkCtxRef;
        errorReporter = errorReporterRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;
        notificationListenerProvider = notificationListenerRef;

        layerFactory = layerFactoryRef;
        storageLayer = storageLayerRef;
        resourceStateEvent = resourceStateEventRef;
        extCmdFactory = extCmdFactoryRef;
        sysFsHandler = sysFsHandlerRef;
        udevHandler = udevHandlerRef;
        snapshotShippingManager = snapshotShippingManagerRef;
        extFileHandler = extFileHandlerRef;
        backupShippingManager = backupShippingManagerRef;
        suspendMgr = suspendMgrRef;
        layerSizeHelper = layerSizeHelperRef;

        suspendMgrRef.setExceptionHandler(this::handleException);

        fullSyncApplied = new AtomicBoolean(false);
    }

    @Override
    public void initialize()
    {
        layerFactory.streamDeviceHandlers().forEach(DeviceLayer::initialize);
    }

    @Override
    public void dispatchResources(Collection<Resource> rscsRef, Collection<Snapshot> snapsRef)
    {
        Collection<Resource> resources = calculateGrossSizes(rscsRef);

        Map<DeviceLayer, Set<AbsRscLayerObject<Resource>>> rscByLayer = groupByLayer(resources);
        Map<DeviceLayer, Set<AbsRscLayerObject<Snapshot>>> snapByLayer = groupByLayer(snapsRef);

        boolean prepareSuccess = prepareLayers(rscByLayer, snapByLayer);

        if (prepareSuccess)
        {
            List<Snapshot> nonDeletingSnapshots = new ArrayList<>();
            List<Snapshot> deletingSnapshots = new ArrayList<>();
            try
            {
                for (Snapshot snap : snapsRef)
                {
                    if (snap.getFlags().isSet(wrkCtx, Snapshot.Flags.DELETE))
                    {
                        deletingSnapshots.add(snap);
                    }
                    else
                    {
                        nonDeletingSnapshots.add(snap);
                    }
                }
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }

            List<Resource> rscListNotifyApplied = new ArrayList<>();
            List<Resource> rscListNotifyDelete = new ArrayList<>();
            List<Volume> vlmListNotifyDelete = new ArrayList<>();
            List<Snapshot> snapListNotifyApplied = new ArrayList<>();
            List<Snapshot> snapListNotifyDelete = new ArrayList<>();

            HashMap<Resource, ApiCallRcImpl> failedRscs = new HashMap<>();

            /*
             * first we need to handle snapshots in DELETING state
             *
             * this should prevent the following error-scenario:
             * deleting a zfs resource still having a snapshot will fail.
             * if the user then tries to delete the snapshot, and this snapshot-deletion is not executed
             * before the resource-deletion, the snapshot will never gets deleted because the resource-deletion
             * will fail before the snapshot-deletion-attempt.
             */
            processSnapshots(
                deletingSnapshots,
                snapListNotifyApplied,
                snapListNotifyDelete,
                failedRscs
            );
            processResources(
                resources,
                rscListNotifyApplied,
                rscListNotifyDelete,
                vlmListNotifyDelete,
                failedRscs
            );
            // now, also process non-deleting snapshots
            processSnapshots(
                nonDeletingSnapshots,
                snapListNotifyApplied,
                snapListNotifyDelete,
                failedRscs
            );

            notifyResourcesApplied(rscListNotifyApplied);
            // no "notifySnapshotApplied" for now - might be added in the future

            NotificationListener listener = notificationListenerProvider.get();
            for (Volume vlm : vlmListNotifyDelete)
            {
                listener.notifyVolumeDeleted(vlm);
            }
            for (Resource rsc : rscListNotifyDelete)
            {
                listener.notifyResourceDeleted(rsc);
            }
            for (Snapshot snap : snapListNotifyDelete)
            {
                listener.notifySnapshotDeleted(snap);
            }

            updateChangedFreeSpaces();

            clearLayerCaches(rscByLayer, snapByLayer);
        }
        else
        {
            // we failed to prepare layers, but we still need to make sure to resume-io if needed so that we do not
            // leave for example DRBD resources in suspended state
            errorReporter.logTrace("Prepare step failed. Checking if devices need resume-io...");
            suspendMgr.manageSuspendIo(rscsRef, true);
        }
    }

    private <RSC extends AbsResource<RSC>> Map<DeviceLayer, Set<AbsRscLayerObject<RSC>>> groupByLayer(
        Collection<RSC> allResources
    )
    {
        Map<DeviceLayer, Set<AbsRscLayerObject<RSC>>> ret = new HashMap<>();
        try
        {
            for (RSC absRsc : allResources)
            {
                AbsRscLayerObject<RSC> rootRscData = absRsc.getLayerData(wrkCtx);

                LinkedList<AbsRscLayerObject<RSC>> toProcess = new LinkedList<>();
                toProcess.add(rootRscData);
                while (!toProcess.isEmpty())
                {
                    AbsRscLayerObject<RSC> rscData = toProcess.poll();
                    toProcess.addAll(rscData.getChildren());

                    DeviceLayer devLayer = layerFactory.getDeviceLayer(rscData.getLayerKind());
                    ret.computeIfAbsent(devLayer, ignored -> new HashSet<>()).add(rscData);
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return ret;
    }

    private ArrayList<Resource> calculateGrossSizes(Collection<Resource> resources) throws ImplementationError
    {
        ArrayList<Resource> rscs = new ArrayList<>();
        try
        {
            for (Resource rsc : resources)
            {
                AbsRscLayerObject<Resource> rscData = rsc.getLayerData(wrkCtx);
                for (VlmProviderObject<Resource> vlmData : rscData.getVlmLayerObjects().values())
                {
                    layerSizeHelper.calculateSize(wrkCtx, vlmData);
                }
                rscs.add(rsc);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return rscs;
    }

    private boolean prepareLayers(
        Map<DeviceLayer, Set<AbsRscLayerObject<Resource>>> rscByLayer,
        Map<DeviceLayer, Set<AbsRscLayerObject<Snapshot>>> snapByLayer
    )
    {
        boolean prepareSuccess = true;

        Set<DeviceLayer> layerSet = SetUtils.mergeIntoHashSet(rscByLayer.keySet(), snapByLayer.keySet());

        for (DeviceLayer layer : layerSet)
        {
            Set<AbsRscLayerObject<Resource>> rscSet = rscByLayer.get(layer);
            Set<AbsRscLayerObject<Snapshot>> snapSet = snapByLayer.get(layer);

            if (rscSet == null)
            {
                rscSet = Collections.emptySet();
            }
            if (snapSet == null)
            {
                snapSet = Collections.emptySet();
            }

            prepareSuccess = prepare(layer, rscSet, snapSet);
            if (!prepareSuccess)
            {
                break;
            }
        }
        return prepareSuccess;
    }

    private void processResources(
        Collection<Resource> resourceList,
        List<Resource> rscListNotifyApplied,
        List<Resource> rscListNotifyDelete,
        List<Volume> vlmListNotifyDelete,
        HashMap<Resource, ApiCallRcImpl> failedRscs

    )
        throws ImplementationError
    {
        failedRscs.putAll(suspendMgr.manageSuspendIo(resourceList, false));

        final NotificationListener notificationListener = notificationListenerProvider.get();
        for (Resource rsc : resourceList)
        {
            ResourceName rscName = rsc.getResourceDefinition().getName();
            ApiCallRcImpl apiCallRc = failedRscs.get(rsc);
            if (apiCallRc == null)
            {
                apiCallRc = new ApiCallRcImpl();
                try
                {

                    AbsRscLayerObject<Resource> rscLayerObject = rsc.getLayerData(wrkCtx);
                    processResource(rscLayerObject, apiCallRc);

                    StateFlags<Flags> rscFlags = rsc.getStateFlags();
                    if (rscFlags.isUnset(wrkCtx, Resource.Flags.DELETE) &&
                        rscFlags.isUnset(wrkCtx, Resource.Flags.INACTIVE))
                    {
                        if (rscLayerObject.getLayerKind().isLocalOnly())
                        {
                            MkfsUtils.makeFileSystemOnMarked(errorReporter, extCmdFactory, wrkCtx, rsc);
                        }
                        updateDiscGran(rscLayerObject);
                    }

                    /*
                     * old device manager reported changes of free space after every
                     * resource operation. As this could require to query the same
                     * VG or zpool multiple times within the same device manager run,
                     * we only query the free space after the whole run.
                     * This also means that we only send the resourceApplied messages
                     * at the very end
                     */
                    if (rscFlags.isSet(wrkCtx, Resource.Flags.DELETE))
                    {
                        rscListNotifyDelete.add(rsc);
                        notificationListener.notifyResourceDeleted(rsc);
                        // rsc.delete is done by the deviceManager
                    }
                    else
                    {
                        Iterator<Volume> iterateVolumes = rsc.iterateVolumes();
                        while (iterateVolumes.hasNext())
                        {
                            Volume vlm = iterateVolumes.next();
                            if (vlm.getFlags().isSet(wrkCtx, Volume.Flags.DELETE))
                            {
                                // verify if all VlmProviderObject were deleted correctly
                                ensureAllVlmDataDeleted(rscLayerObject, vlm.getVolumeDefinition().getVolumeNumber());
                                vlmListNotifyDelete.add(vlm);
                            }
                            else
                            {
                                updateDeviceSymlinks(vlm);
                            }
                        }
                        rscListNotifyApplied.add(rsc);
                    }

                    extFileHandler.handle(rsc);

                    // give the layer the opportunity to send a "resource ready" event
                    AbsRscLayerObject<Resource> firstNonIgnoredRscData = getFirstRscDataWithoutIgnoredReasonForDataPath(
                        rsc.getLayerData(wrkCtx)
                    );
                    if (firstNonIgnoredRscData == null)
                    {
                        Set<LayerIgnoreReason> ignoreReasons = rsc.getLayerData(wrkCtx).getIgnoreReasons();
                        errorReporter.logDebug(
                            "Not calling resourceFinished for any layer as the resource '%s' is completely ignored. " +
                                "Topmost reason%s: %s",
                            rsc.getLayerData(wrkCtx).getSuffixedResourceName(),
                            ignoreReasons.size() > 1 ? "s" : "",
                            LayerIgnoreReason.getDescriptions(ignoreReasons)
                        );
                    }
                    else
                    {
                        resourceFinished(firstNonIgnoredRscData);
                    }

                    if (Platform.isLinux())
                    {
                        if (rscFlags.isUnset(wrkCtx, Resource.Flags.DELETE))
                        {
                            sysFsHandler.update(rsc, apiCallRc);
                        }
                        else
                        {
                            sysFsHandler.cleanup(rsc);
                        }
                    }
                }
                catch (AccessDeniedException | DatabaseException exc)
                {
                    throw new ImplementationError(exc);
                }
                catch (Exception | ImplementationError exc)
                {
                    apiCallRc = handleException(rsc, exc);
                }
            }
            notificationListener.notifyResourceDispatchResponse(rscName, apiCallRc);
        }
    }

    private ApiCallRcImpl handleException(Resource rsc, Throwable exc)
    {
        ResourceName rscName = rsc.getResourceDefinition().getName();
        ApiCallRcImpl apiCallRc;
        String errorId = errorReporter.reportError(
            exc,
            null,
            null,
            "An error occurred while processing resource '" + rsc + "'"
        );

        long rc;
        String errMsg;
        String cause;
        String correction;
        String details;
        if (exc instanceof StorageException ||
            exc instanceof ResourceException ||
            exc instanceof VolumeException
        )
        {
            LinStorException linExc = (LinStorException) exc;
            // TODO add returnCode and message to the classes StorageException, ResourceException and
            // VolumeException and include them here

            rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            errMsg = exc.getMessage();

            cause = linExc.getCauseText();
            correction = linExc.getCorrectionText();
            details = linExc.getDetailsText();
        }
        else
        if (exc instanceof AbortLayerProcessingException)
        {
            AbsRscLayerObject<?> rscLayerData = ((AbortLayerProcessingException) exc).rscLayerObject;
            rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            errMsg = exc.getMessage();

            if (errMsg == null)
            {
                errMsg = String.format(
                    "Layer '%s' failed to process resource '%s'. ",
                    rscLayerData.getLayerKind().name(),
                    rscLayerData.getSuffixedResourceName()
                );
            }

            cause = null;
            correction = null;

            List<String> devLayersAbove = new ArrayList<>();
            AbsRscLayerObject<?> parent = rscLayerData.getParent();
            while (parent != null)
            {
                devLayersAbove.add(layerFactory.getDeviceLayer(parent.getLayerKind()).getName());
                parent = parent.getParent();
            }
            details = String.format("Skipping layers above %s", devLayersAbove);
        }
        else
        {
            rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            errMsg = exc.getMessage();
            if (errMsg == null)
            {
                errMsg = "An unknown exception occurred while processing the resource " + rscName.displayValue;
            }

            cause = null;
            correction = null;
            details = null;
        }

        apiCallRc = ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
            .entryBuilder(rc, errMsg)
            .setCause(cause)
            .setCorrection(correction)
            .setDetails(details)
            .addErrorId(errorId)
            .build()
        );

        notificationListenerProvider.get().notifyResourceFailed(rsc, apiCallRc);
        return apiCallRc;
    }

    private <RSC extends AbsResource<RSC>> AbsRscLayerObject<RSC> getFirstRscDataWithoutIgnoredReasonForDataPath(
        AbsRscLayerObject<RSC> rscData
    )
    {
        AbsRscLayerObject<RSC> curRscData = rscData;
        while (curRscData != null && curRscData.hasAnyPreventExecutionIgnoreReason())
        {
            curRscData = curRscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);
        }
        return curRscData;
    }

    private void ensureAllVlmDataDeleted(
        AbsRscLayerObject<Resource> rscLayerObjectRef,
        VolumeNumber volumeNumberRef
    )
        throws ImplementationError
    {
        VlmProviderObject<Resource> vlmData = rscLayerObjectRef.getVlmProviderObject(volumeNumberRef);
        if (vlmData.exists())
        {
            throw new ImplementationError("Layer '" + rscLayerObjectRef.getLayerKind() + " did not delete the volume " +
                volumeNumberRef + " of resource " + rscLayerObjectRef.getSuffixedResourceName() + " properly");
        }
        for (AbsRscLayerObject<Resource> child : rscLayerObjectRef.getChildren())
        {
            ensureAllVlmDataDeleted(child, volumeNumberRef);
        }
    }

    private void processSnapshots(
        List<Snapshot> snapshots,
        List<Snapshot> snapListNotifyApplied,
        List<Snapshot> snapListNotifyDelete,
        HashMap<Resource, ApiCallRcImpl> failedRscs
    )
        throws ImplementationError
    {
        for (Snapshot snap : snapshots)
        {
            @Nullable ApiCallRcImpl apiCallRc;
            try
            {
                Resource rsc = snap.getResourceDefinition().getResource(wrkCtx, snap.getNodeName());
                apiCallRc = failedRscs.get(rsc);
                boolean process;
                if (apiCallRc == null)
                {
                    process = true;
                    apiCallRc = new ApiCallRcImpl();
                }
                else
                {
                    // The deviceHandler processes first deleting snapshots, then resources and at last also
                    // non-deleting snapshots.
                    // That means, that deleting snapshots cannot get into this "else" case, since the resources were
                    // not processed yet and thus had no chance to write anything into "failedRscs".
                    process = !apiCallRc.hasErrors();
                }
                if (process)
                {
                    processSnapshot(snap.getLayerData(wrkCtx), apiCallRc);

                    if (snap.getFlags().isSet(wrkCtx, Snapshot.Flags.DELETE))
                    {
                        snapListNotifyDelete.add(snap);
                        // snapshot.delete is done by the deviceManager
                    }
                    else
                    {
                        // start the snapshot-shipping-daemons and backup-shipping-daemons if necessary
                        snapshotShippingManager.allSnapshotPartsRegistered(snap);
                        snapListNotifyApplied.add(snap);
                        backupShippingManager.allBackupPartsRegistered(snap);
                    }
                }
            }
            catch (AccessDeniedException | DatabaseException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (StorageException | ResourceException | VolumeException exc)
            {
                // TODO different handling for different exceptions?
                String errorId = errorReporter.reportError(
                    exc,
                    null,
                    null,
                    "An error occurred while processing snapshot '" + snap.getSnapshotName() + "' of resource '" +
                        snap.getResourceName() + "'"
                );

                apiCallRc = ApiCallRcImpl.singletonApiCallRc(
                    ApiCallRcImpl
                        .entryBuilder(
                            // TODO maybe include a ret-code into the exception
                            ApiConsts.FAIL_UNKNOWN_ERROR,
                            exc.getMessage()
                        )
                        .setCause(exc.getCauseText())
                        .setCorrection(exc.getCorrectionText())
                        .setDetails(exc.getDetailsText())
                        .addErrorId(errorId)
                        .build()
                );
            }
            catch (RuntimeException exc)
            {
                String errorId = errorReporter.reportError(
                    exc,
                    null,
                    null,
                    "An error occurred while processing snapshot '" + snap.getSnapshotName() + "' of resource '" +
                        snap.getResourceName() + "'"
                );

                apiCallRc = ApiCallRcImpl.singletonApiCallRc(
                    ApiCallRcImpl
                        .entryBuilder(
                            // TODO maybe include a ret-code into the exception
                            ApiConsts.FAIL_UNKNOWN_ERROR,
                            exc.getMessage()
                        )
                        .addErrorId(errorId)
                        .build()
                );
            }
            notificationListenerProvider.get()
                .notifySnapshotDispatchResponse(snap.getSnapshotDefinition().getSnapDfnKey(), apiCallRc);
        }
    }

    private void notifyResourcesApplied(List<Resource> rscListNotifyApplied) throws ImplementationError
    {
        Peer ctrlPeer = controllerPeerConnector.getControllerPeer();
        if (ctrlPeer != null)
        {
            try
            {
                Map<StorPoolInfo, Either<SpaceInfo, ApiRcException>> spaceInfoQueryMap =
                    storageLayer.getFreeSpaceOfAccessedStoagePools();

                Map<StorPoolInfo, SpaceInfo> spaceInfoMap = new TreeMap<>();

                spaceInfoQueryMap.forEach((storPool, either) -> either.consume(
                    spaceInfo -> spaceInfoMap.put(storPool, spaceInfo),
                    apiRcException -> errorReporter.reportError(apiRcException.getCause())
                ));

                // TODO: rework API answer
                /*
                 * Instead of sending single change, request and applied / deleted messages per
                 * resource, the controller and satellite should use one message containing
                 * multiple resources.
                 * The final message regarding applied and/or deleted resource can so also contain
                 * the new free spaces of the affected storage pools
                 */

                for (Resource rsc : rscListNotifyApplied)
                {
                    ctrlPeer.sendMessage(
                        interComSerializer
                            .onewayBuilder(InternalApiConsts.API_NOTIFY_RSC_APPLIED)
                            .notifyResourceApplied(rsc, spaceInfoMap)
                            .build(),
                        InternalApiConsts.API_NOTIFY_RSC_APPLIED
                    );
                }
            }
            catch (AccessDeniedException accDeniedExc)
            {
                throw new ImplementationError(accDeniedExc);
            }
        }
    }

    private void clearLayerCaches(
        Map<DeviceLayer, Set<AbsRscLayerObject<Resource>>> rscByLayer,
        Map<DeviceLayer, Set<AbsRscLayerObject<Snapshot>>> snapByLayer
    )
    {
        Set<DeviceLayer> layers = SetUtils.mergeIntoHashSet(rscByLayer.keySet(), snapByLayer.keySet());

        final NotificationListener notificationListener = notificationListenerProvider.get();
        for (DeviceLayer layer : layers)
        {
            try
            {
                layer.clearCache();
            }
            catch (StorageException exc)
            {
                errorReporter.reportError(exc);
                ApiCallRcImpl apiCallRc = ApiCallRcImpl.singletonApiCallRc(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_UNKNOWN_ERROR,
                        "An error occurred while cleaning up layer '" + layer.getName() + "'"
                    )
                    .setCause(exc.getCauseText())
                    .setCorrection(exc.getCorrectionText())
                    .setDetails(exc.getDetailsText())
                    .build()
                );
                for (AbsRscLayerObject<Resource> rsc : rscByLayer.get(layer))
                {
                    notificationListener.notifyResourceDispatchResponse(
                        rsc.getResourceName(),
                        apiCallRc
                    );
                }
            }
        }
    }

    private boolean prepare(
        DeviceLayer layer,
        Set<AbsRscLayerObject<Resource>> rscSet,
        Set<AbsRscLayerObject<Snapshot>> snapSet
    )
    {
        boolean success;
        try
        {
            Set<AbsRscLayerObject<Resource>> filteredRscSet = filterNonIgnored(rscSet);
            Set<AbsRscLayerObject<Snapshot>> filteredSnapSet = filterNonIgnored(snapSet);
            errorReporter.logTrace(
                "Layer '%s' preparing %d (of %d) resources, %d (of %d) snapshots",
                layer.getName(),
                filteredRscSet.size(),
                rscSet.size(),
                filteredSnapSet.size(),
                snapSet.size()
            );

            layer.prepare(filteredRscSet, filteredSnapSet);
            errorReporter.logTrace(
                "Layer '%s' finished preparing %d resources, %d snapshots",
                layer.getName(),
                filteredRscSet.size(),
                filteredSnapSet.size()
            );
            success = true;
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (Exception exc)
        {
            success = false;
            errorReporter.reportError(exc);

            EntryBuilder builder = ApiCallRcImpl.entryBuilder(
                // TODO maybe include a ret-code into the StorageException
                ApiConsts.FAIL_UNKNOWN_ERROR,
                "Preparing resources for layer " + layer.getName() + " failed"
            );
            if (exc instanceof LinStorException)
            {
                LinStorException linstorExc = (LinStorException) exc;
                builder = builder
                    .setCause(linstorExc.getCauseText())
                    .setCorrection(linstorExc.getCorrectionText())
                    .setDetails(linstorExc.getDetailsText());
            }

            ApiCallRcImpl apiCallRc = ApiCallRcImpl.singletonApiCallRc(
                builder.build()
            );
            final NotificationListener notificationListener = notificationListenerProvider.get();
            for (AbsRscLayerObject<Resource> failedResource : rscSet)
            {
                notificationListener.notifyResourceFailed(
                    failedResource.getAbsResource(),
                    apiCallRc
                );
            }
        }
        return success;
    }

    private <RSC extends AbsResource<RSC>> Set<AbsRscLayerObject<RSC>> filterNonIgnored(
        Set<AbsRscLayerObject<RSC>> dataSetRef
    )
    {
        Set<AbsRscLayerObject<RSC>> ret = new HashSet<>();
        for (AbsRscLayerObject<RSC> data : dataSetRef)
        {
            Set<LayerIgnoreReason> ignoreReasons = data.getIgnoreReasons();
            boolean processRscData = !data.hasAnyPreventExecutionIgnoreReason();
            errorReporter.logTrace(
                "%s: %s has reason%s: %s. %s",
                data.getLayerKind(),
                data.getSuffixedResourceName(),
                ignoreReasons.size() > 1 ? "s" : "",
                LayerIgnoreReason.getDescriptions(ignoreReasons),
                processRscData ? "Processing" : "Skipping"
            );
            if (processRscData)
            {
                ret.add(data);
            }
        }
        return ret;
    }

    private void resourceFinished(AbsRscLayerObject<Resource> layerDataRef)
    {
        AbsRscLayerObject<Resource> layerData = layerDataRef;

        boolean resourceReadySent = false;
        while (!resourceReadySent)
        {
            DeviceLayer rootLayer = layerFactory.getDeviceLayer(layerData.getLayerKind());
            if (!layerData.hasFailed())
            {
                try
                {
                    resourceReadySent = rootLayer.resourceFinished(layerData);
                    layerData = layerData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);
                }
                catch (AccessDeniedException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
            else
            {
                errorReporter.logDebug(
                    "Not calling resourceFinished for layer %s as the resource '%s' failed",
                    rootLayer.getName(),
                    layerDataRef.getSuffixedResourceName()
                );
                resourceReadySent = true; // resource failed, will not be ready
            }
        }
    }

    @Override
    public void sendResourceCreatedEvent(AbsRscLayerObject<Resource> layerDataRef, ResourceState resourceStateRef)
    {
        resourceStateEvent.get().triggerEvent(
            ObjectIdentifier.resourceDefinition(layerDataRef.getResourceName()),
            resourceStateRef
        );
    }

    @Override
    public void sendResourceDeletedEvent(AbsRscLayerObject<Resource> layerDataRef)
    {
        resourceStateEvent.get().closeStream(
            ObjectIdentifier.resourceDefinition(layerDataRef.getResourceName())
        );
    }

    @Override
    public void processResource(AbsRscLayerObject<Resource> rscLayerDataRef, ApiCallRcImpl apiCallRcRef)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        processGeneric(
            rscLayerDataRef,
            apiCallRcRef,
            (layer, layerData, apiCallRc) -> {
                layer.processResource(layerData, apiCallRc);
                return true;
            },
            layerData -> String.format("resource '%s'", layerData.getSuffixedResourceName())
        );
    }

    @Override
    public void processSnapshot(AbsRscLayerObject<Snapshot> snapLayerDataRef, ApiCallRcImpl apiCallRcRef)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        processGeneric(
            snapLayerDataRef,
            apiCallRcRef,
            DeviceLayer::processSnapshot,
            layerData -> String.format(
                "snapshot '%s' of resource '%s'",
                layerData.getSnapName().displayValue,
                layerData.getSuffixedResourceName()
            )
        );
    }

    private <RSC extends AbsResource<RSC>> void processGeneric(
        AbsRscLayerObject<RSC> rscLayerData,
        ApiCallRcImpl apiCallRc,
        ProcessInterface<RSC> procFctRef,
        Function<AbsRscLayerObject<RSC>, String> dataDescrFct
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        boolean processedChildren = false;
        DeviceLayer currentLayer = layerFactory.getDeviceLayer(rscLayerData.getLayerKind());

        if (!rscLayerData.hasAnyPreventExecutionIgnoreReason())
        {
            errorReporter.logTrace(
                "Layer '%s' processing %s",
                currentLayer.getName(),
                dataDescrFct.apply(rscLayerData)
            );
            processedChildren = procFctRef.process(currentLayer, rscLayerData, apiCallRc);
        }
        /*
         * Even if the current rscLayerData was skipped due to ignoreReasons, we might need to process one of its
         * children (i.e. an NVMe target rscData).
         *
         * Usually the order of processing children is important, but not if the current rscData is ignored.
         */
        if (!processedChildren)
        {
            errorReporter.logTrace(
                "Ignoring layer '%s' for %s because '%s'",
                currentLayer.getName(),
                dataDescrFct.apply(rscLayerData),
                LayerIgnoreReason.getDescriptions(rscLayerData.getIgnoreReasons())
            );
            for (AbsRscLayerObject<RSC> child : rscLayerData.getChildren())
            {
                processGeneric(child, apiCallRc, procFctRef, dataDescrFct);
            }
        }

        if (rscLayerData.hasFailed())
        {
            throw new AbortLayerProcessingException(rscLayerData);
        }

        errorReporter.logTrace(
            "Layer '%s' finished processing resource '%s'",
            currentLayer.getName(),
            rscLayerData.getSuffixedResourceName()
        );
    }

    // TODO: create delete volume / resource methods that (for now) only perform the actual .delete()
    // command. This method should be a central point for future logging or other extensions / purposes

    @Override
    public void fullSyncApplied(Node localNode) throws StorageException
    {
        fullSyncApplied.set(true);
        try
        {
            localNodeProps = localNode.getProps(wrkCtx);
            localNodePropsChanged(localNodeProps);

            extFileHandler.clear();
            extFileHandler.rebuildExtFilesToRscDfnMaps(localNode);

            backupShippingManager.killAllShipping();
            snapshotShippingManager.killAllShipping();
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    // FIXME: this method also needs to be called when the localnode's properties change, not just
    // (as currently) when a fullSync was applied
    @Override
    public void localNodePropsChanged(Props newLocalNodeProps) throws StorageException, AccessDeniedException
    {
        LocalPropsChangePojo collectedChanged = new LocalPropsChangePojo();

        Iterator<DeviceLayer> devHandlerIt = layerFactory.iterateDeviceHandlers();
        while (devHandlerIt.hasNext())
        {
            DeviceLayer deviceLayer = devHandlerIt.next();
            LocalPropsChangePojo pojo = deviceLayer.setLocalNodeProps(newLocalNodeProps);

            // TODO we could implement a safeguard here such that a layer can only change/delete properties
            // from its own namespace.

            if (pojo != null)
            {
                collectedChanged.putAll(pojo);
            }
        }

        if (!collectedChanged.isEmpty())
        {
            controllerPeerConnector.getControllerPeer().sendMessage(
                interComSerializer
                    .onewayBuilder(InternalApiConsts.API_UPDATE_LOCAL_PROPS_FROM_STLT)
                    .updateLocalProps(collectedChanged)
                    .build(),
                InternalApiConsts.API_UPDATE_LOCAL_PROPS_FROM_STLT
            );
        }
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPoolInfo storPoolInfo, boolean update) throws StorageException
    {
        SpaceInfo spaceInfo;
        try
        {
            DeviceLayer layer;
            switch (storPoolInfo.getDeviceProviderKind())
            {
                case DISKLESS: // fall-through
                case FILE: // fall-through
                case FILE_THIN: // fall-through
                case LVM: // fall-through
                case LVM_THIN: // fall-through
                case SPDK: // fall-through
                case REMOTE_SPDK: // fall-through
                case ZFS: // fall-through
                case ZFS_THIN: // fall-through
                case EBS_INIT: // fall-through
                case EBS_TARGET: // fall-through
                case EXOS: // fall-through
                case STORAGE_SPACES: // fall-through
                case STORAGE_SPACES_THIN:
                    layer = storageLayer;
                    break;
                case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                default:
                    throw new ImplementationError("Unknown provider kind: " + storPoolInfo.getDeviceProviderKind());
            }

            LocalPropsChangePojo pojo = layer.checkStorPool(storPoolInfo, update);
            spaceInfo = layer.getStoragePoolSpaceInfo(storPoolInfo);

            if (pojo != null)
            {
                controllerPeerConnector.getControllerPeer().sendMessage(
                    interComSerializer
                        .onewayBuilder(InternalApiConsts.API_UPDATE_LOCAL_PROPS_FROM_STLT)
                        .updateLocalProps(pojo)
                        .build(),
                    InternalApiConsts.API_UPDATE_LOCAL_PROPS_FROM_STLT
                );
            }
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        return spaceInfo;
    }

    private void updateChangedFreeSpaces()
    {
        Map<StorPool, SpaceInfo> freeSpaces = new TreeMap<>();
        for (StorPool storPool : storageLayer.getChangedStorPools())
        {
            try
            {
                freeSpaces.put(
                    storPool,
                    storageLayer.getStoragePoolSpaceInfo(storPool)
                );
            }
            catch (StorageException exc)
            {
                errorReporter.logError("Failed to query freespace or capacity of storPool " + storPool.getName());
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
        notificationListenerProvider.get().notifyFreeSpacesChanged(freeSpaces);
    }

    private void updateDeviceSymlinks(Volume vlmRef)
        throws AccessDeniedException, StorageException, DatabaseException, InvalidKeyException, InvalidValueException
    {
        List<String> prefixedList = new ArrayList<>();

        if (!vlmRef.getAbsResource().getStateFlags().isSet(wrkCtx, Resource.Flags.INACTIVE))
        {
            // if resource is inactive, prefixedList stays empty. this will effectively clear the volume props-namespace
            // of the symlinks

            VlmProviderObject<Resource> vlmProviderObject = vlmRef.getAbsResource()
                .getLayerData(wrkCtx)
                .getVlmProviderObject(vlmRef.getVolumeNumber());

            TreeSet<String> symlinks = null;
            if (!vlmRef.getFlags().isSet(wrkCtx, Volume.Flags.CLONING))
            {
                symlinks = udevHandler.getSymlinks(vlmProviderObject.getDevicePath());
            }

            if (symlinks != null)
            {
                // elements of symlinks usually do not start with "/dev/"
                for (String entry : symlinks)
                {
                    if (!entry.startsWith("/dev/"))
                    {
                        prefixedList.add("/dev/" + entry);
                    }
                    else
                    {
                        prefixedList.add(entry);
                    }
                }
            }
        }

        Props vlmProps = vlmRef.getProps(wrkCtx);
        @Nullable Props symlinkProps = vlmProps.getNamespace(ApiConsts.NAMESPC_STLT_DEV_SYMLINKS);
        if (symlinkProps != null)
        {
            symlinkProps.clear();
        }
        for (int idx = 0; idx < prefixedList.size(); idx++)
        {
            String symlink = prefixedList.get(idx);
            vlmProps.setProp(Integer.toString(idx), symlink, ApiConsts.NAMESPC_STLT_DEV_SYMLINKS);
        }
    }

    private void updateDiscGran(AbsRscLayerObject<Resource> rscLayerObjectRef)
        throws DatabaseException, StorageException, AccessDeniedException
    {
        if (!Platform.isWindows() && rscLayerObjectRef != null)
        {
            DeviceLayer layer = layerFactory.getDeviceLayer(rscLayerObjectRef.getLayerKind());
            if (!rscLayerObjectRef.hasAnyPreventExecutionIgnoreReason() && layer.isDiscGranFeasible(rscLayerObjectRef))
            {
                for (VlmProviderObject<Resource> vlmData : rscLayerObjectRef.getVlmLayerObjects().values())
                {
                    updateDiscGran(vlmData);
                }
            }
            for (AbsRscLayerObject<Resource> childRscData : rscLayerObjectRef.getChildren())
            {
                updateDiscGran(childRscData);
            }
        }
    }

    private void updateDiscGran(VlmProviderObject<Resource> vlmData) throws DatabaseException, StorageException
    {
        String devicePath = vlmData.getDevicePath();
        if (devicePath != null && vlmData.exists())
        {
            if (vlmData.getDiscGran() == VlmProviderObject.UNINITIALIZED_SIZE)
            {
                int retryCount = LSBLK_DISC_GRAN_RETRY_COUNT;
                boolean discGranUpdated = false;
                while (!discGranUpdated)
                {
                    try
                    {
                        List<LsBlkEntry> lsblkEntry = LsBlkUtils.lsblk(extCmdFactory.create(), devicePath);
                        vlmData.setDiscGran(lsblkEntry.get(0).getDiscGran());
                        discGranUpdated = true;
                    }
                    catch (StorageException exc)
                    {
                        retryCount--;
                        if (retryCount == 0)
                        {
                            throw exc;
                        }
                        try
                        {
                            Thread.sleep(LSBLK_DISC_GRAN_RETRY_TIMEOUT_IN_MS);
                        }
                        catch (InterruptedException exc1)
                        {
                            exc1.printStackTrace();
                        }
                    }
                }
            }
        }
        else
        {
            vlmData.setDiscGran(VlmProviderObject.UNINITIALIZED_SIZE);
        }
    }

    private interface ProcessInterface<RSC extends AbsResource<RSC>>
    {
        boolean process(DeviceLayer layer, AbsRscLayerObject<RSC> layerData, ApiCallRcImpl apiCallRc)
            throws StorageException, ResourceException, VolumeException, AccessDeniedException,
            DatabaseException, AbortLayerProcessingException;
    }
}
