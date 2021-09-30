package com.linbit.linstor.core.devmgr;

import com.linbit.ImplementationError;
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
import com.linbit.linstor.core.devmgr.pojos.LocalNodePropsChangePojo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.DeviceLayer.AbortLayerProcessingException;
import com.linbit.linstor.layer.DeviceLayer.LayerProcessResult;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.LayerFactory;
import com.linbit.linstor.layer.openflex.OpenflexLayer;
import com.linbit.linstor.layer.storage.StorageLayer;
import com.linbit.linstor.layer.storage.utils.MkfsUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.utils.SetUtils;
import com.linbit.utils.Either;

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
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Singleton
public class DeviceHandlerImpl implements DeviceHandler
{
    private final AccessContext wrkCtx;
    private final ErrorReporter errorReporter;
    private final Provider<NotificationListener> notificationListener;

    private final ControllerPeerConnector controllerPeerConnector;
    private final CtrlStltSerializer interComSerializer;
    private final ResourceStateEvent resourceStateEvent;

    private final LayerFactory layerFactory;
    private final AtomicBoolean fullSyncApplied;
    private final OpenflexLayer openflexLayer;
    private final StorageLayer storageLayer;
    private final ExtCmdFactory extCmdFactory;
    private final SnapshotShippingService snapshotShippingManager;

    private final SysFsHandler sysFsHandler;
    private final UdevHandler udevHandler;
    private final StltExternalFileHandler extFileHandler;

    private Props localNodeProps;
    private BackupShippingMgr backupShippingManager;

    @Inject
    public DeviceHandlerImpl(
        @DeviceManagerContext AccessContext wrkCtxRef,
        ErrorReporter errorReporterRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer interComSerializerRef,
        Provider<NotificationListener> notificationListenerRef,
        LayerFactory layerFactoryRef,
        OpenflexLayer openflexLayerRef,
        StorageLayer storageLayerRef,
        ResourceStateEvent resourceStateEventRef,
        ExtCmdFactory extCmdFactoryRef,
        SysFsHandler sysFsHandlerRef,
        UdevHandler udevHandlerRef,
        SnapshotShippingService snapshotShippingManagerRef,
        StltExternalFileHandler extFileHandlerRef,
        BackupShippingMgr backupShippingManagerRef
    )
    {
        wrkCtx = wrkCtxRef;
        errorReporter = errorReporterRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;
        notificationListener = notificationListenerRef;

        layerFactory = layerFactoryRef;
        openflexLayer = openflexLayerRef;
        storageLayer = storageLayerRef;
        resourceStateEvent = resourceStateEventRef;
        extCmdFactory = extCmdFactoryRef;
        sysFsHandler = sysFsHandlerRef;
        udevHandler = udevHandlerRef;
        snapshotShippingManager = snapshotShippingManagerRef;
        extFileHandler = extFileHandlerRef;
        backupShippingManager = backupShippingManagerRef;

        fullSyncApplied = new AtomicBoolean(false);
    }

    @Override
    public void initialize()
    {
        layerFactory.streamDeviceHandlers().forEach(DeviceLayer::initialize);
    }

    @Override
    public void dispatchResources(Collection<Resource> rscsRef, Collection<Snapshot> snapshots)
    {
        Collection<Resource> resources = calculateGrossSizes(rscsRef);

        Map<DeviceLayer, Set<AbsRscLayerObject<Resource>>> rscByLayer = groupByLayer(resources);
        Map<DeviceLayer, Set<AbsRscLayerObject<Snapshot>>> snapByLayer = groupByLayer(snapshots);

        boolean prepareSuccess = prepareLayers(rscByLayer, snapByLayer);

        if (prepareSuccess)
        {
            List<Snapshot> unprocessedSnapshots = new ArrayList<>(snapshots);

            List<Resource> rscListNotifyApplied = new ArrayList<>();
            List<Resource> rscListNotifyDelete = new ArrayList<>();
            List<Volume> vlmListNotifyDelete = new ArrayList<>();
            List<Snapshot> snapListNotifyDelete = new ArrayList<>();

            processResourcesAndSnapshots(
                resources,
                snapshots,
                unprocessedSnapshots,
                rscListNotifyApplied,
                rscListNotifyDelete,
                vlmListNotifyDelete,
                snapListNotifyDelete
            );
            processUnprocessedSnapshots(
                unprocessedSnapshots,
                snapListNotifyDelete
            );

            notifyResourcesApplied(rscListNotifyApplied);

            NotificationListener listener = notificationListener.get();
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
                try
                {
                    AbsRscLayerObject<Resource> rscData = rsc.getLayerData(wrkCtx);
                    for (VlmProviderObject<Resource> vlmData : rscData.getVlmLayerObjects().values())
                    {
                        long vlmDfnSize = vlmData.getVolume().getVolumeSize(wrkCtx);
                        boolean calculateNetSizes = vlmData.getVolume().getVolumeDefinition().getFlags()
                            .isSet(wrkCtx, VolumeDefinition.Flags.GROSS_SIZE);

                        if (calculateNetSizes)
                        {
                            vlmData.setAllocatedSize(vlmDfnSize);
                            updateUsableSizeFromAllocatedSize(vlmData);
                            vlmData.setOriginalSize(vlmDfnSize);
                        }
                        else
                        {
                            vlmData.setUsableSize(vlmDfnSize);
                            updateAllocatedSizeFromUsableSize(vlmData);
                            vlmData.setOriginalSize(vlmDfnSize);
                        }
                    }
                    rscs.add(rsc);
                }
                catch (StorageException exc)
                {
                    String errorId = errorReporter.reportError(
                        exc,
                        null,
                        null,
                        "An error occurred while calculating gross size of resource '" + rsc + "'"
                    );
                    ApiCallRcImpl apiCallRc = ApiCallRcImpl.singletonApiCallRc(
                        ApiCallRcImpl
                            .entryBuilder(ApiConsts.FAIL_UNKNOWN_ERROR, exc.getMessage())
                            .setCause(exc.getCauseText())
                            .setCorrection(exc.getCorrectionText())
                            .setDetails(exc.getDetailsText())
                            .addErrorId(errorId)
                            .build()
                    );

                    notificationListener.get().notifyResourceFailed(rsc, apiCallRc);
                }
            }
        }
        catch (AccessDeniedException | DatabaseException exc)
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

    private void processResourcesAndSnapshots(
        Collection<Resource> resourceList,
        Collection<Snapshot> snapshotsRef,
        List<Snapshot> unprocessedSnapshotsRef,
        List<Resource> rscListNotifyApplied,
        List<Resource> rscListNotifyDelete,
        List<Volume> vlmListNotifyDelete,
        List<Snapshot> snapListNotifyDelete
    )
        throws ImplementationError
    {
        Map<ResourceName, List<Snapshot>> snapshotsByRscName = snapshotsRef.stream()
            .collect(Collectors.groupingBy(Snapshot::getResourceName));

        List<Resource> sysFsUpdateList = new ArrayList<>();
        List<Resource> sysFsDeleteList = new ArrayList<>();

        for (Resource rsc : resourceList)
        {
            ResourceName rscName = rsc.getDefinition().getName();

            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            try
            {
                List<Snapshot> snapshots = snapshotsByRscName.get(rscName);
                if (snapshots == null)
                {
                    snapshots = Collections.emptyList();
                }
                unprocessedSnapshotsRef.removeAll(snapshots);

                AbsRscLayerObject<Resource> rscLayerObject = rsc.getLayerData(wrkCtx);
                process(
                    rscLayerObject,
                    snapshots,
                    apiCallRc
                );

                StateFlags<Flags> rscFlags = rsc.getStateFlags();
                if (
                    rscLayerObject.getLayerKind().isLocalOnly() &&
                        rscFlags.isUnset(wrkCtx, Resource.Flags.DELETE) &&
                        rscFlags.isUnset(wrkCtx, Resource.Flags.INACTIVE)
                )
                {
                    MkfsUtils.makeFileSystemOnMarked(errorReporter, extCmdFactory, wrkCtx, rsc);
                }
                for (Snapshot snapshot : snapshots)
                {
                    if (snapshot.getFlags().isSet(wrkCtx, Snapshot.Flags.DELETE))
                    {
                        snapListNotifyDelete.add(snapshot);
                        // snapshot.delete is done by the deviceManager
                    }
                    else
                    {
                        // start the snapshot-shipping-daemons and backup-shipping-daemons if necessary
                        snapshotShippingManager.allSnapshotPartsRegistered(snapshot);
                        backupShippingManager.allBackupPartsRegistered(snapshot);
                    }
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
                    notificationListener.get().notifyResourceDeleted(rsc);
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
                resourceFinished(rsc.getLayerData(wrkCtx));

                if (rscFlags.isUnset(wrkCtx, Resource.Flags.DELETE))
                {
                    sysFsUpdateList.add(rsc);
                }
                else
                {
                    sysFsDeleteList.add(rsc);
                }
            }
            catch (AccessDeniedException | DatabaseException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (Exception | ImplementationError exc)
            {
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

                notificationListener.get().notifyResourceFailed(rsc, apiCallRc);
            }
            notificationListener.get().notifyResourceDispatchResponse(rscName, apiCallRc);
        }
        sysFsHandler.updateSysFsSettings(sysFsUpdateList, sysFsDeleteList);
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

    private void processUnprocessedSnapshots(
        List<Snapshot> unprocessedSnapshots,
        List<Snapshot> snapListNotifyDelete
    )
        throws ImplementationError
    {
        Map<ResourceName, List<Snapshot>> snapshotsByResourceName = unprocessedSnapshots.stream()
            .collect(Collectors.groupingBy(Snapshot::getResourceName));
        /*
         *  We cannot use the .process(Resource, List<Snapshot>, ApiCallRc) method as we do not have a
         *  resource. The resource is used for determining which DeviceLayer to use, thus would result in
         *  a NPE.
         *  However, actually we know that there are no resources "based" on these snapshots (else the
         *  DeviceManager would have found them and called the previous case, such that those snapshots
         *  would have been processed already).
         *  That means, we can skip all layers and go directory to the StorageLayer, which, fortunately,
         *  does not need a resource for processing snapshots.
         */
        for (Entry<ResourceName, List<Snapshot>> entry : snapshotsByResourceName.entrySet())
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            try
            {
                storageLayer.process(
                    null, // no resource, no rscLayerData
                    entry.getValue(), // list of snapshots
                    apiCallRc
                );

                for (Snapshot snapshot : entry.getValue())
                {
                    if (snapshot.getFlags().isSet(wrkCtx, Snapshot.Flags.DELETE))
                    {
                        snapListNotifyDelete.add(snapshot);
                        // snapshot.delete is done by the deviceManager
                    }
                    else
                    {
                        backupShippingManager.allBackupPartsRegistered(snapshot);
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
                    "An error occurred while processing resource '" + entry.getKey() + "'"
                );

                apiCallRc = ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
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
                    "An error occurred while processing resource '" + entry.getKey() + "'"
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
            notificationListener.get().notifyResourceDispatchResponse(entry.getKey(), apiCallRc);
        }
    }

    private void notifyResourcesApplied(List<Resource> rscListNotifyApplied) throws ImplementationError
    {
        Peer ctrlPeer = controllerPeerConnector.getControllerPeer();
        if (ctrlPeer != null)
        {
            try
            {
                Map<StorPool, Either<SpaceInfo, ApiRcException>> spaceInfoQueryMap =
                    storageLayer.getFreeSpaceOfAccessedStoagePools();

                Map<StorPool, SpaceInfo> spaceInfoMap = new TreeMap<>();

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
                            .build()
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
                    notificationListener.get().notifyResourceDispatchResponse(
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
            errorReporter.logTrace(
                "Layer '%s' preparing %d resources, %d snapshots",
                layer.getName(),
                rscSet.size(),
                snapSet.size()
            );
            layer.prepare(rscSet, snapSet);
            errorReporter.logTrace(
                "Layer '%s' finished preparing %d resources, %d snapshots",
                layer.getName(),
                rscSet.size(),
                snapSet.size()
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
            for (AbsRscLayerObject<Resource> failedResource : rscSet)
            {
                notificationListener.get().notifyResourceFailed(
                    failedResource.getAbsResource(),
                    apiCallRc
                );
            }
        }
        return success;
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
    public LayerProcessResult process(
        AbsRscLayerObject<Resource> rscLayerData,
        List<Snapshot> snapshotsRef,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        DeviceLayer nextLayer = layerFactory.getDeviceLayer(rscLayerData.getLayerKind());

        errorReporter.logTrace(
            "Layer '%s' processing resource '%s'",
            nextLayer.getName(),
            rscLayerData.getSuffixedResourceName()
        );

        LayerProcessResult processResult = nextLayer.process(rscLayerData, snapshotsRef, apiCallRc);

        if (rscLayerData.hasFailed())
        {
            throw new AbortLayerProcessingException(rscLayerData);
        }

        errorReporter.logTrace(
            "Layer '%s' finished processing resource '%s'",
            nextLayer.getName(),
            rscLayerData.getSuffixedResourceName()
        );
        return processResult;
    }

    @Override
    public void updateAllocatedSizeFromUsableSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        DeviceLayer nextLayer = layerFactory.getDeviceLayer(vlmData.getLayerKind());

        errorReporter.logTrace(
            "Layer '%s' updating gross size of volume '%s/%d' (usable: %d)",
            nextLayer.getName(),
            vlmData.getRscLayerObject().getSuffixedResourceName(),
            vlmData.getVlmNr().value,
            vlmData.getUsableSize()
        );

        nextLayer.updateAllocatedSizeFromUsableSize(vlmData);

        errorReporter.logTrace(
            "Layer '%s' finished calculating sizes of volume '%s/%d'. Allocated: %d, usable: %d",
            nextLayer.getName(),
            vlmData.getRscLayerObject().getSuffixedResourceName(),
            vlmData.getVlmNr().value,
            vlmData.getAllocatedSize(),
            vlmData.getUsableSize()
        );
    }

    @Override
    public void updateUsableSizeFromAllocatedSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        DeviceLayer nextLayer = layerFactory.getDeviceLayer(vlmData.getLayerKind());

        errorReporter.logTrace(
            "Layer '%s' updating net size of volume '%s/%d' (allocated: %d)",
            nextLayer.getName(),
            vlmData.getRscLayerObject().getSuffixedResourceName(),
            vlmData.getVlmNr().value,
            vlmData.getAllocatedSize()
        );

        nextLayer.updateUsableSizeFromAllocatedSize(vlmData);

        errorReporter.logTrace(
            "Layer '%s' finished calculating sizes of volume '%s/%d'. Allocated: %d, usable: %d",
            nextLayer.getName(),
            vlmData.getRscLayerObject().getSuffixedResourceName(),
            vlmData.getVlmNr().value,
            vlmData.getAllocatedSize(),
            vlmData.getUsableSize()
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
    public void localNodePropsChanged(Props localNodeProps) throws StorageException, AccessDeniedException
    {
        Map<String, String> changedLocalNodeProps = new HashMap<>();
        Set<String> deletedLocalNodeProps = new HashSet<>();

        Iterator<DeviceLayer> devHandlerIt = layerFactory.iterateDeviceHandlers();
        while (devHandlerIt.hasNext())
        {
            DeviceLayer deviceLayer = devHandlerIt.next();
            LocalNodePropsChangePojo pojo = deviceLayer.setLocalNodeProps(localNodeProps);

            // TODO we could implement a safeguard here such that a layer can only change/delete properties
            // from its own namespace.

            if (pojo != null)
            {
                changedLocalNodeProps.putAll(pojo.changedProps);
                deletedLocalNodeProps.addAll(pojo.deletedProps);
            }
        }

        if (!changedLocalNodeProps.isEmpty() || !deletedLocalNodeProps.isEmpty())
        {
            controllerPeerConnector.getControllerPeer().sendMessage(
                interComSerializer
                    .onewayBuilder(InternalApiConsts.API_UPDATE_LOCAL_NODE_PROPS_FROM_STLT)
                    .updateLocalNodeProps(changedLocalNodeProps, deletedLocalNodeProps)
                    .build()
            );
        }
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPool storPool, boolean update) throws StorageException
    {
        SpaceInfo spaceInfo;
        try
        {
            DeviceLayer layer;
            switch (storPool.getDeviceProviderKind())
            {
                case OPENFLEX_TARGET:
                    layer = openflexLayer;
                    break;
                case DISKLESS: // fall-through
                case FILE: // fall-through
                case FILE_THIN: // fall-through
                case LVM: // fall-through
                case LVM_THIN: // fall-through
                case SPDK: // fall-through
                case REMOTE_SPDK: // fall-through
                case ZFS: // fall-through
                case ZFS_THIN: // fall-through
                case EXOS:
                    layer = storageLayer;
                    break;
                case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                default:
                    throw new ImplementationError("Unknown provider kind: " + storPool.getDeviceProviderKind());
            }

            LocalNodePropsChangePojo pojo = layer.checkStorPool(storPool, update);
            spaceInfo = layer.getStoragePoolSpaceInfo(storPool);

            if (pojo != null)
            {
                controllerPeerConnector.getControllerPeer().sendMessage(
                    interComSerializer
                        .onewayBuilder(InternalApiConsts.API_UPDATE_LOCAL_NODE_PROPS_FROM_STLT)
                        .updateLocalNodeProps(pojo.changedProps, pojo.deletedProps)
                        .build()
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
        notificationListener.get().notifyFreeSpacesChanged(freeSpaces);
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

            List<String> symlinks = null;
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
        Optional<Props> symlinkProps = vlmProps.getNamespace(ApiConsts.NAMESPC_STLT_DEV_SYMLINKS);
        if (symlinkProps.isPresent())
        {
            symlinkProps.get().clear();
        }
        for (int i = 0; i < prefixedList.size(); i++)
        {
            String symlink = prefixedList.get(i);
            vlmProps.setProp(Integer.toString(i), symlink, ApiConsts.NAMESPC_STLT_DEV_SYMLINKS);
        }
    }
}
