package com.linbit.linstor.core.devmgr;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.SysFsHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.Resource.RscFlags;
import com.linbit.linstor.core.objects.Snapshot.SnapshotFlags;
import com.linbit.linstor.core.objects.Volume.VlmFlags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.LayerFactory;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
import com.linbit.linstor.storage.layer.provider.StorageLayer;
import com.linbit.linstor.storage.utils.MkfsUtils;
import com.linbit.utils.Either;
import com.linbit.utils.Pair;

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
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
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
    private final StorageLayer storageLayer;
    private final ExtCmdFactory extCmdFactory;

    private final SysFsHandler sysFsHandler;

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
        SysFsHandler sysFsHandlerRef
    )
    {
        wrkCtx = wrkCtxRef;
        errorReporter = errorReporterRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;
        notificationListener = notificationListenerRef;

        layerFactory = layerFactoryRef;
        storageLayer = storageLayerRef;
        resourceStateEvent = resourceStateEventRef;
        extCmdFactory = extCmdFactoryRef;
        sysFsHandler = sysFsHandlerRef;

        fullSyncApplied = new AtomicBoolean(false);
    }

    @Override
    public void dispatchResources(Collection<Resource> rscs, Collection<Snapshot> snapshots)
    {
        /*
         * TODO: we may need to add some snapshotname-suffix logic (like for resourcename)
         * when implementing snapshots for / through RAID-layer
         */
        Map<DeviceLayer, Set<RscLayerObject>> rscByLayer = groupResourcesByLayer(rscs);
        Map<ResourceName, Set<Snapshot>> snapshotsByRscName = groupSnapshotsByResourceName(snapshots);

        calculateGrossSizes(rscs);

        boolean prepareSuccess = prepareLayers(rscByLayer, snapshots);

        if (prepareSuccess)
        {
            List<Snapshot> unprocessedSnapshots = new ArrayList<>(snapshots);

            List<Resource> rscListNotifyApplied = new ArrayList<>();
            List<Resource> rscListNotifyDelete = new ArrayList<>();
            List<Volume> vlmListNotifyDelete = new ArrayList<>();
            List<Snapshot> snapListNotifyDelete = new ArrayList<>();

            processResourcesAndTheirSnapshots(
                snapshots,
                rscs,
                snapshotsByRscName,
                unprocessedSnapshots,
                rscListNotifyApplied,
                rscListNotifyDelete,
                vlmListNotifyDelete,
                snapListNotifyDelete
            );
            processUnprocessedSnapshots(unprocessedSnapshots);

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

            updateChangedFreeSpaces();

            clearLayerCaches(rscByLayer);
        }
    }

    private Map<DeviceLayer, Set<RscLayerObject>> groupResourcesByLayer(Collection<Resource> allResources)
    {
        Map<DeviceLayer, Set<RscLayerObject>> ret = new HashMap<>();
        try
        {
            for (Resource rsc : allResources)
            {
                RscLayerObject rootRscData = rsc.getLayerData(wrkCtx);

                LinkedList<RscLayerObject> toProcess = new LinkedList<>();
                toProcess.add(rootRscData);
                while (!toProcess.isEmpty())
                {
                    RscLayerObject rscData = toProcess.poll();
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

    private Map<ResourceName, Set<Snapshot>> groupSnapshotsByResourceName(Collection<Snapshot> snapshots)
    {
        return snapshots.stream().collect(
            Collectors.groupingBy(
                Snapshot::getResourceName,
                Collectors.mapping(
                    Function.identity(),
                    Collectors.toSet()
                )
            )
        );
    }

    private void calculateGrossSizes(Collection<Resource> rootResources) throws ImplementationError
    {
        for (Resource rsc : rootResources)
        {
            try
            {
                updateGrossSizeForChildren(rsc);
            }
            catch (AccessDeniedException | DatabaseException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    private boolean prepareLayers(
        Map<DeviceLayer, Set<RscLayerObject>> rscByLayer,
        Collection<Snapshot> snapshots
    )
    {
        boolean prepareSuccess = true;

        Map<DeviceLayer, Pair<Set<RscLayerObject>, Set<Snapshot>>> dataPerLayer = new HashMap<>();
        for (Entry<DeviceLayer, Set<RscLayerObject>> entry : rscByLayer.entrySet())
        {
            dataPerLayer.put(entry.getKey(), new Pair<>(entry.getValue(), new HashSet<>()));
        }
        if (!snapshots.isEmpty())
        {
            Pair<Set<RscLayerObject>, Set<Snapshot>> storageLayerObjectPair = dataPerLayer.get(storageLayer);
            if (storageLayerObjectPair == null)
            {
                storageLayerObjectPair = new Pair<>(new HashSet<>(), new HashSet<>());
                dataPerLayer.put(storageLayer, storageLayerObjectPair);
            }
            Set<Snapshot> snapshotsToPrepare = storageLayerObjectPair.objB;
            snapshotsToPrepare.addAll(snapshots);
        }


        for (Entry<DeviceLayer, Pair<Set<RscLayerObject>, Set<Snapshot>>> entry : dataPerLayer.entrySet())
        {
            DeviceLayer layer = entry.getKey();
            Pair<Set<RscLayerObject>, Set<Snapshot>> pair = entry.getValue();
            if (!prepare(layer, pair.objA, pair.objB))
            {
                prepareSuccess = false;
                break;
            }
        }
        return prepareSuccess;
    }

    private void processResourcesAndTheirSnapshots(
        Collection<Snapshot> snapshots,
        Collection<Resource> resourceList,
        Map<ResourceName, Set<Snapshot>> snapshotsByRscName,
        List<Snapshot> unprocessedSnapshots,
        List<Resource> rscListNotifyApplied,
        List<Resource> rscListNotifyDelete,
        List<Volume> vlmListNotifyDelete,
        List<Snapshot> snapListNotifyDelete
    )
        throws ImplementationError
    {
        for (Resource rsc : resourceList)
        {
            ResourceName rscName = rsc.getDefinition().getName();

            Set<Snapshot> snapshotList = snapshotsByRscName.getOrDefault(rscName, Collections.emptySet());
            unprocessedSnapshots.removeAll(snapshotList);

            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            try
            {
                RscLayerObject rscLayerObject = rsc.getLayerData(wrkCtx);
                process(
                    rscLayerObject,
                    snapshotList,
                    apiCallRc
                );

                if (rscLayerObject.getLayerKind().isLocalOnly() &&
                    rsc.getStateFlags().isUnset(wrkCtx, Resource.RscFlags.DELETE))
                {
                    MkfsUtils.makeFileSystemOnMarked(errorReporter, extCmdFactory, wrkCtx, rsc);
                }

                /*
                 * old device manager reported changes of free space after every
                 * resource operation. As this could require to query the same
                 * VG or zpool multiple times within the same device manager run,
                 * we only query the free space after the whole run.
                 * This also means that we only send the resourceApplied messages
                 * at the very end
                 */
                if (rsc.getStateFlags().isSet(wrkCtx, RscFlags.DELETE))
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
                        if (vlm.getFlags().isSet(wrkCtx, VlmFlags.DELETE))
                        {
                            // verify if all VlmProviderObject were deleted correctly
                            ensureAllVlmDataDeleted(rscLayerObject, vlm.getVolumeDefinition().getVolumeNumber());
                            vlmListNotifyDelete.add(vlm);
                        }
                    }
                    rscListNotifyApplied.add(rsc);
                }

                for (Snapshot snapshot : snapshots)
                {
                    if (snapshot.getFlags().isSet(wrkCtx, SnapshotFlags.DELETE))
                    {
                        snapListNotifyDelete.add(snapshot);
                        notificationListener.get().notifySnapshotDeleted(snapshot);
                        // snapshot.delete is done by the deviceManager
                    }
                }

                // give the layer the opportunity to send a "resource ready" event
                resourceFinished(rsc.getLayerData(wrkCtx));
            }
            catch (AccessDeniedException | DatabaseException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (AbortLayerProcessingException exc)
            {
                RscLayerObject rscLayerData = exc.rscLayerObject;
                List<String> devLayersAbove = new ArrayList<>();
                RscLayerObject parent = rscLayerData.getParent();
                while (parent != null)
                {
                    devLayersAbove.add(layerFactory.getDeviceLayer(parent.getLayerKind()).getName());
                    parent = parent.getParent();
                }

                errorReporter.logError(
                    "Layer '%s' failed to process resource '%s'. Skipping layers above %s",
                    rscLayerData.getLayerKind().name(),
                    rscLayerData.getSuffixedResourceName(),
                    devLayersAbove
                );
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
        sysFsHandler.updateSysFsSettings(resourceList);
    }

    private void ensureAllVlmDataDeleted(RscLayerObject rscLayerObjectRef, VolumeNumber volumeNumberRef)
        throws ImplementationError
    {
        VlmProviderObject vlmData = rscLayerObjectRef.getVlmProviderObject(volumeNumberRef);
        if (vlmData.exists())
        {
            throw new ImplementationError("Layer '" + rscLayerObjectRef.getLayerKind() + " did not delete the volume " +
                volumeNumberRef + " of resource " + rscLayerObjectRef.getSuffixedResourceName() + " properly");
        }
        for (RscLayerObject child : rscLayerObjectRef.getChildren())
        {
            ensureAllVlmDataDeleted(child, volumeNumberRef);
        }
    }

    private void processUnprocessedSnapshots(List<Snapshot> unprocessedSnapshots) throws ImplementationError
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

    private void clearLayerCaches(Map<DeviceLayer, Set<RscLayerObject>> rscByLayer)
    {
        for (Entry<DeviceLayer, Set<RscLayerObject>> entry : rscByLayer.entrySet())
        {
            DeviceLayer layer = entry.getKey();
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
                for (RscLayerObject rsc : entry.getValue())
                {
                    notificationListener.get().notifyResourceDispatchResponse(
                        rsc.getResourceName(),
                        apiCallRc
                    );
                }
            }
        }
    }

    private boolean prepare(DeviceLayer layer, Set<RscLayerObject> rscDataList, Set<Snapshot> affectedSnapshots)
    {
        boolean success;
        try
        {
            errorReporter.logTrace(
                "Layer '%s' preparing %d resources, %d snapshots",
                layer.getName(),
                rscDataList.size(),
                affectedSnapshots.size()
            );
            layer.prepare(rscDataList, affectedSnapshots);
            errorReporter.logTrace(
                "Layer '%s' finished preparing %d resources, %d snapshots",
                layer.getName(),
                rscDataList.size(),
                affectedSnapshots.size()
            );
            success = true;
        }
        catch (StorageException exc)
        {
            success = false;
            errorReporter.reportError(exc);

            ApiCallRcImpl apiCallRc = ApiCallRcImpl.singletonApiCallRc(
                ApiCallRcImpl.entryBuilder(
                    // TODO maybe include a ret-code into the StorageException
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "Preparing resources for layer " + layer.getName() + " failed"
                )
                .setCause(exc.getCauseText())
                .setCorrection(exc.getCorrectionText())
                .setDetails(exc.getDetailsText())
                .build()
            );
            for (RscLayerObject failedResourceData : rscDataList)
            {
                notificationListener.get().notifyResourceDispatchResponse(
                    failedResourceData.getResourceName(),
                    apiCallRc
                );
            }
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        return success;
    }

    private void resourceFinished(RscLayerObject layerDataRef)
    {
        DeviceLayer rootLayer = layerFactory.getDeviceLayer(layerDataRef.getLayerKind());
        if (!layerDataRef.hasFailed())
        {
            try
            {
                rootLayer.resourceFinished(layerDataRef);
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
        }
    }

    @Override
    public void sendResourceCreatedEvent(RscLayerObject layerDataRef, UsageState usageStateRef)
    {
        resourceStateEvent.get().triggerEvent(
            ObjectIdentifier.resourceDefinition(layerDataRef.getResourceName()),
            usageStateRef
        );
    }

    @Override
    public void sendResourceDeletedEvent(RscLayerObject layerDataRef)
    {
        resourceStateEvent.get().closeStream(
            ObjectIdentifier.resourceDefinition(layerDataRef.getResourceName())
        );
    }

    @Override
    public void process(
        RscLayerObject rscLayerData,
        Collection<Snapshot> snapshots,
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

        nextLayer.process(rscLayerData, snapshots, apiCallRc);

        if (rscLayerData.hasFailed())
        {
            throw new AbortLayerProcessingException(rscLayerData);
        }

        errorReporter.logTrace(
            "Layer '%s' finished processing resource '%s'",
            nextLayer.getName(),
            rscLayerData.getSuffixedResourceName()
        );
    }

    public void updateGrossSizeForChildren(Resource rsc) throws AccessDeniedException, DatabaseException
    {
        for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
        {
            long size = vlm.getVolumeDefinition().getVolumeSize(wrkCtx);
            { // set initial size which will be changed by the actual layers shortly
                vlm.setAllocatedSize(wrkCtx, size);
                vlm.setUsableSize(wrkCtx, size);
            }

            VolumeNumber vlmNr = vlm.getVolumeDefinition().getVolumeNumber();

            LinkedList<RscLayerObject> rscDataList = new LinkedList<>();
            rscDataList.add(rsc.getLayerData(wrkCtx));

            VlmProviderObject vlmData = null;
            boolean isVlmDataRoot = true;

            while (!rscDataList.isEmpty())
            {
                RscLayerObject rscData = rscDataList.removeFirst();
                rscDataList.addAll(rscData.getChildren());

                vlmData = rscData.getVlmProviderObject(vlmNr);
                if (vlmData != null)
                {
                    if (isVlmDataRoot)
                    {
                        /*
                         * set the usableSize for the root vlmData.
                         * All layers are expected to set their own gross size (allocated size)
                         * as well as their child-vlmData's usableSize
                         */
                        vlmData.setUsableSize(size);
                        isVlmDataRoot = false;
                    }
                    layerFactory.getDeviceLayer(rscData.getLayerKind()).updateGrossSize(vlmData);
                }
            }
        }
    }

    // TODO: create delete volume / resource methods that (for now) only perform the actual .delete()
    // command. This method should be a central point for future logging or other extensions / purposes

    public void fullSyncApplied(Node localNode)
    {
        fullSyncApplied.set(true);
        try
        {
            Props localNodeProps = localNode.getProps(wrkCtx);
            localNodePropsChanged(localNodeProps);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    // FIXME: this method also needs to be called when the localnode's properties change, not just
    // (as currently) when a fullSync was applied
    public void localNodePropsChanged(Props localNodeProps)
    {
        layerFactory.streamDeviceHandlers().forEach(
            rscLayer ->
                rscLayer.setLocalNodeProps(localNodeProps));
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
                    new SpaceInfo(
                        storageLayer.getCapacity(storPool),
                        storageLayer.getFreeSpace(storPool)
                    )
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

    private final class AbortLayerProcessingException extends LinStorRuntimeException
    {
        private static final long serialVersionUID = -3885415188860635819L;
        private RscLayerObject rscLayerObject;

        private AbortLayerProcessingException(RscLayerObject rscLayerObjectRef)
        {
            super(
                String.format("Layer '%s' aborted by failed resource '%s'",
                    rscLayerObjectRef.getLayerKind().name(),
                    rscLayerObjectRef.getSuffixedResourceName()
                )
            );
            rscLayerObject = rscLayerObjectRef;
        }
    }
}
