package com.linbit.linstor.core.devmgr;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.Snapshot.SnapshotFlags;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.devmgr.helper.LayeredResourcesHelper;
import com.linbit.linstor.core.devmgr.helper.LayeredSnapshotHelper;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.LayerFactory;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.ResourceLayer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
import com.linbit.linstor.storage.layer.provider.StorageLayer;
import com.linbit.utils.Either;
import com.linbit.utils.RemoveAfterDevMgrRework;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Singleton
public class DeviceHandlerImpl implements DeviceHandler2
{
    private final AccessContext wrkCtx;
    private final ErrorReporter errorReporter;
    private final Provider<NotificationListener> notificationListener;

    private final ControllerPeerConnector controllerPeerConnector;
    private final CtrlStltSerializer interComSerializer;

    private final LayeredResourcesHelper layeredRscHelper;
    private final LayeredSnapshotHelper layeredSnapshotHelper;
    private final LayerFactory layerFactory;
    private final AtomicBoolean fullSyncApplied;
    private final StorageLayer storageLayer;

    @Inject
    public DeviceHandlerImpl(
        @DeviceManagerContext AccessContext wrkCtxRef,
        ErrorReporter errorReporterRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer interComSerializerRef,
        Provider<NotificationListener> notificationListenerRef,
        LayeredResourcesHelper layeredRscHelperRef,
        LayeredSnapshotHelper layeredSnapshotHelperRef,
        LayerFactory layerFactoryRef,
        StorageLayer storageLayerRef
    )
    {
        wrkCtx = wrkCtxRef;
        errorReporter = errorReporterRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;
        notificationListener = notificationListenerRef;

        layeredRscHelper = layeredRscHelperRef;
        layeredSnapshotHelper = layeredSnapshotHelperRef;
        layerFactory = layerFactoryRef;
        storageLayer = storageLayerRef;

        fullSyncApplied = new AtomicBoolean(false);
    }

    @Override
    public void dispatchResources(Collection<Resource> rscs, Collection<Snapshot> snapshots)
    {
        Collection<Resource> origResources = rscs;
        List<Resource> allResources = convertResources(origResources);
        updateSnapshotLayerData(origResources, snapshots);

        Set<Resource> rootResources = origResources.stream().map(rsc -> getRoot(rsc)).collect(Collectors.toSet());
        Map<ResourceLayer, List<Resource>> rscByLayer = allResources.stream()
            .collect(
                Collectors.groupingBy(
                    rsc -> layerFactory.getDeviceLayer(rsc.getType().getDevLayerKind().getClass())
                )
            );
        Map<ResourceName, List<Snapshot>> snapshotsByRscName =
            snapshots.stream().collect(Collectors.groupingBy(Snapshot::getResourceName));

        // call prepare for every necessary layer
        boolean prepareSuccess = true;
        for (Entry<ResourceLayer, List<Resource>> entry : rscByLayer.entrySet())
        {
            List<Snapshot> affectedSnapshots = new ArrayList<>();
            List<Resource> rscList = entry.getValue();
            for (Resource rsc : rscList)
            {
                List<Snapshot> list = snapshotsByRscName.get(rsc.getDefinition().getName());
                if (list != null)
                {
                    affectedSnapshots.addAll(list);
                }
            }
            ResourceLayer layer = entry.getKey();
            if (!prepare(layer, rscList, affectedSnapshots))
            {
                prepareSuccess = false;
                break;
            }
        }

        // calculate gross sizes
        for (Resource rsc : rootResources)
        {
            try
            {
                updateGrossSizeForChildren(rsc);
            }
            catch (AccessDeniedException | SQLException exc)
            {
                throw new ImplementationError(exc);
            }
        }


        if (prepareSuccess)
        {
            List<Snapshot> unprocessedSnapshots = new ArrayList<>(snapshots);

            List<Resource> rscListNotifyApplied = new ArrayList<>();
            List<Resource> rscListNotifyDelete = new ArrayList<>();
            List<Snapshot> snapListNotifyDelete = new ArrayList<>();

            // actually process every resource and snapshots
            for (Resource rsc : rootResources)
            {
                ResourceName rscName = rsc.getDefinition().getName();
                ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                try
                {
                    List<Snapshot> snapshotList = snapshotsByRscName.get(rscName);
                    if (snapshotList == null)
                    {
                        snapshotList = Collections.emptyList();
                    }
                    process(
                        rsc,
                        snapshotList,
                        apiCallRc
                    );
                    unprocessedSnapshots.removeAll(snapshotList);

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
                        rscListNotifyApplied.add(rsc);
                        notificationListener.get().notifyResourceApplied(rsc);
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
                }
                catch (AccessDeniedException | SQLException exc)
                {
                    throw new ImplementationError(exc);
                }
                catch (StorageException | ResourceException | VolumeException exc)
                {
                    // TODO different handling for different exceptions?
                    errorReporter.reportError(exc);

                    apiCallRc = ApiCallRcImpl.singletonApiCallRc(
                        ApiCallRcImpl.entryBuilder(
                            // TODO maybe include a ret-code into the exception
                            ApiConsts.FAIL_UNKNOWN_ERROR,
                            "An error occured while processing resource '" + rsc + "'"
                        )
                        .setCause(exc.getCauseText())
                        .setCorrection(exc.getCorrectionText())
                        .setDetails(exc.getDetailsText())
                        .build()
                    );
                }
                notificationListener.get().notifyResourceDispatchResponse(rscName, apiCallRc);
            }

            // process unprocessed snapshots
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
                 *  That means, we can skip all layers and go directoy to the StorageLayer, which, fortunately,
                 *  does not need a resource for processing snapshots.
                 */
                for (Entry<ResourceName, List<Snapshot>> entry : snapshotsByResourceName.entrySet())
                {
                    ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                    try
                    {
                        storageLayer.process(null, entry.getValue(), apiCallRc);
                    }
                    catch (AccessDeniedException | SQLException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                    catch (StorageException | ResourceException | VolumeException exc)
                    {
                        // TODO different handling for different exceptions?
                        errorReporter.reportError(exc);

                        apiCallRc = ApiCallRcImpl.singletonApiCallRc(
                            ApiCallRcImpl.entryBuilder(
                                // TODO maybe include a ret-code into the exception
                                ApiConsts.FAIL_UNKNOWN_ERROR,
                                "An error occured while processing resource '" + entry.getKey() + "'"
                            )
                            .setCause(exc.getCauseText())
                            .setCorrection(exc.getCorrectionText())
                            .setDetails(exc.getDetailsText())
                            .build()
                        );
                    }
                    notificationListener.get().notifyResourceDispatchResponse(entry.getKey(), apiCallRc);
                }
            }

            // query changed storage pools and send out all notifications
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

            // call clear cache for every layer where the .prepare was called
            for (Entry<ResourceLayer, List<Resource>> entry : rscByLayer.entrySet())
            {
                ResourceLayer layer = entry.getKey();
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
                            "An error occured while cleaning up layer '" + layer.getName() + "'"
                        )
                        .setCause(exc.getCauseText())
                        .setCorrection(exc.getCorrectionText())
                        .setDetails(exc.getDetailsText())
                        .build()
                    );
                    for (Resource rsc : entry.getValue())
                    {
                        notificationListener.get().notifyResourceDispatchResponse(
                            rsc.getDefinition().getName(),
                            apiCallRc
                        );
                    }
                }
            }

            layeredRscHelper.cleanupResources(origResources);
        }
    }

    private Resource getRoot(Resource rsc)
    {
        Resource root = rsc;
        try
        {
            Resource tmp = rsc.getParentResource(wrkCtx);
            while (tmp != null)
            {
                root = tmp;
                tmp = tmp.getParentResource(wrkCtx);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return root;
    }

    private boolean prepare(ResourceLayer layer, List<Resource> resources, List<Snapshot> affectedSnapshots)
    {
        boolean success;
        try
        {
            errorReporter.logTrace(
                "Layer '%s' preparing %d resources",
                layer.getName(),
                resources.size()
            );
            layer.prepare(resources, affectedSnapshots);
            errorReporter.logTrace(
                "Layer '%s' finished preparing %d resources",
                layer.getName(),
                resources.size()
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
            for (Resource failedResource : resources)
            {
                notificationListener.get().notifyResourceDispatchResponse(
                    failedResource.getDefinition().getName(),
                    apiCallRc
                );
            }
        }
        catch (AccessDeniedException | SQLException exc)
        {
            throw new ImplementationError(exc);
        }
        return success;
    }

    @Override
    public void process(Resource rsc, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException,
            AccessDeniedException, SQLException
    {
        ResourceLayer devLayer = layerFactory.getDeviceLayer(rsc.getType().getDevLayerKind().getClass());
        errorReporter.logTrace(
            "Layer '%s' processing resource '%s'",
            devLayer.getName(),
            rsc.getKey().toString()
        );
        devLayer.process(rsc, snapshots, apiCallRc);
        errorReporter.logTrace(
            "Layer '%s' finished processing resource '%s'",
            devLayer.getName(),
            rsc.getKey().toString()
        );
    }

    public void updateGrossSizeForChildren(Resource dfltRsc) throws AccessDeniedException, SQLException
    {
        LinkedList<Resource> resources = new LinkedList<>();
        resources.add(dfltRsc);
        while (!resources.isEmpty())
        {
            Resource parent = resources.pollFirst();
            for (Resource child : parent.getChildResources(wrkCtx))
            {
                ResourceLayer devLayer = layerFactory.getDeviceLayer(child.getType().getDevLayerKind().getClass());

                for (Volume childVlm : child.streamVolumes().collect(Collectors.toList()))
                {
                    devLayer.updateGrossSize(
                        childVlm,
                        parent.getVolume(childVlm.getVolumeDefinition().getVolumeNumber())
                    );
                }

                resources.add(child);
            }
        }
    }

    // TODO: create delete volume / resource mehtods that (for now) only perform the actual .delete()
    // command. This method should be a central point for future logging or other extensions / purposes

    /**
     * This method splits one {@link Resource} into device-layer-specific resources.
     * In future versions of LINSTOR this method should get obsolete as the API layer should
     * already receive the correct resources.
     * @throws AccessDeniedException
     */
    @RemoveAfterDevMgrRework
    private List<Resource> convertResources(Collection<Resource> resourcesToProcess)
    {
        // convert resourceNames to resources
        return layeredRscHelper.extractLayers(resourcesToProcess);
    }

    @RemoveAfterDevMgrRework
    private void updateSnapshotLayerData(Collection<Resource> dfltResources, Collection<Snapshot> snapshots)
    {
        layeredSnapshotHelper.updateSnapshotLayerData(dfltResources, snapshots);
    }

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

    public void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException
    {
        storageLayer.checkStorPool(storPool);
    }
}
