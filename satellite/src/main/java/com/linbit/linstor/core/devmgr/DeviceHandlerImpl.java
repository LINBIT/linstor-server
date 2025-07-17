package com.linbit.linstor.core.devmgr;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.Platform;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.EntryBuilder;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.StltExternalFileHandler;
import com.linbit.linstor.core.SysFsHandler;
import com.linbit.linstor.core.UdevHandler;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
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
import com.linbit.linstor.layer.DeviceLayer.CloneSupportResult;
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
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.LsBlkEntry;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.MkfsUtils;
import com.linbit.linstor.utils.SetUtils;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
    private final CloneService cloneService;

    private final SysFsHandler sysFsHandler;
    private final UdevHandler udevHandler;
    private final StltExternalFileHandler extFileHandler;

    private @Nullable Props localNodeProps;
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
        StltExternalFileHandler extFileHandlerRef,
        BackupShippingMgr backupShippingManagerRef,
        SuspendManager suspendMgrRef,
        LayerSizeHelper layerSizeHelperRef,
        CloneService cloneServiceRef
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
        extFileHandler = extFileHandlerRef;
        backupShippingManager = backupShippingManagerRef;
        suspendMgr = suspendMgrRef;
        layerSizeHelper = layerSizeHelperRef;
        cloneService = cloneServiceRef;

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
            processClone(
                resources,
                rscListNotifyApplied,
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

        TreeSet<DeviceLayer> layerTreeSet = SetUtils.mergeIntoTreeSet(rscByLayer.keySet(), snapByLayer.keySet());

        for (DeviceLayer layer : layerTreeSet)
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
                        rscFlags.isUnset(wrkCtx, Resource.Flags.INACTIVE) &&
                        rsc.getResourceDefinition().getFlags().isUnset(wrkCtx, ResourceDefinition.Flags.CLONING))
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
                        Iterator<Volume> iterateVolumes = rsc.iterateVolumes();
                        while (iterateVolumes.hasNext())
                        {
                            Volume vlm = iterateVolumes.next();
                            // verify if all VlmProviderObject were deleted correctly
                            ensureAllVlmDataDeleted(rscLayerObject, vlm.getVolumeDefinition().getVolumeNumber());
                            vlmListNotifyDelete.add(vlm);
                        }
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
                    @Nullable AbsRscLayerObject<Resource> firstNonIgnoredRscData = getFirstRscDataToExecuteForDataPath(
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
                        if (rscFlags.isUnset(wrkCtx, Resource.Flags.DELETE, Flags.DRBD_DELETE))
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
                errMsg = "An exception of type " + exc.getClass().getSimpleName() +
                    " occurred while processing the resource " + rscName.displayValue;
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

    /**
     * Returns the first (if any) layer resource data, following ONLY the {@link RscLayerSuffixes#SUFFIX_DATA} path that
     * should not be ignored. The latter means that either no set {@link LayerIgnoreReason} wants to prevent execution
     * or if all those {@link LayerIgnoreReason} have an exception when the resource is in deleting state, and the
     * resource is indeed in deleting state. This method can return <code>null</code> if all layer resource data should
     * be ignored.
     *
     * @param <RSC>
     * @param rscData
     *
     * @return
     *
     * @throws AccessDeniedException
     */
    private <RSC extends AbsResource<RSC>> @Nullable AbsRscLayerObject<RSC> getFirstRscDataToExecuteForDataPath(
        AbsRscLayerObject<RSC> rscData
    )
        throws AccessDeniedException
    {
        @Nullable AbsRscLayerObject<RSC> curRscData = rscData;
        final boolean shouldAbsRscBeDeleted = isAbsRscDeleteFlagSet(curRscData);
        while (curRscData != null && !shouldRscDataBeProcessed(curRscData, shouldAbsRscBeDeleted))
        {
            curRscData = curRscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);
        }
        return curRscData;
    }

    private <RSC extends AbsResource<RSC>> boolean shouldRscDataBeProcessed(
        AbsRscLayerObject<RSC> curRscData,
        final boolean shouldAbsRscBeDeleted
    )
        throws AccessDeniedException
    {
        boolean processResource = true;
        for (LayerIgnoreReason ignoreReason : curRscData.getIgnoreReasons())
        {
            if (ignoreReason.isPreventExecution())
            {
                if (shouldAbsRscBeDeleted || hasLayerSpecificDeleteFlagSet(curRscData))
                {
                    if (ignoreReason.isPreventExecutionWhenDeleting())
                    {
                        processResource = false;
                    }
                }
                else
                {
                    processResource = false;
                }
            }
            if (!processResource)
            {
                // cannot get true anymore
                break;
            }
        }
        return processResource;
    }

    private <RSC extends AbsResource<RSC>> boolean isAbsRscDeleteFlagSet(AbsRscLayerObject<RSC> rscDataRef)
        throws AccessDeniedException
    {
        final boolean ret;
        final RSC absRsc = rscDataRef.getAbsResource();
        if (absRsc instanceof Resource)
        {
            ret = ((Resource) absRsc).getStateFlags().isSet(wrkCtx, Resource.Flags.DELETE);
        }
        else
        {
            ret = ((Snapshot) absRsc).getFlags().isSet(wrkCtx, Snapshot.Flags.DELETE);
        }
        return ret;
    }

    private <RSC extends AbsResource<RSC>> boolean hasLayerSpecificDeleteFlagSet(AbsRscLayerObject<RSC> rscDataRef)
        throws AccessDeniedException
    {
        return layerFactory.getDeviceLayer(rscDataRef.getLayerKind()).isDeleteFlagSet(rscDataRef);
    }

    private <RSC extends AbsResource<RSC>> boolean shouldExceptionBeIgnored(
        AbsRscLayerObject<RSC> rscLayerDataRef
    )
        throws AccessDeniedException
    {
        boolean ret;
        Set<LayerIgnoreReason> ignoreReasons = rscLayerDataRef.getIgnoreReasons();
        if (ignoreReasons.isEmpty())
        {
            ret = false; // do not ignore exception without ignore reason
        }
        else
        {
            boolean isDeleting = isAbsRscDeleteFlagSet(rscLayerDataRef) ||
                hasLayerSpecificDeleteFlagSet(rscLayerDataRef);
            if (isDeleting)
            {
                // if we have ignore reasons, still tried to delete the resource and got an exception in the
                // progress, we want to ignore the exception by default, unless an ignore-reason that wanted
                // the processing does not want to ignore the exception
                ret = true;
                for (LayerIgnoreReason ignoreReason : ignoreReasons)
                {
                    if (ignoreReason.isPreventExecutionWhenDeleting() &&
                        !ignoreReason.isSuppressErrorOnDelete())
                    {
                        ret = false;
                        break;
                    }
                }
            }
            else
            {
                ret = false;
            }
        }
        return ret;
    }

    private void ensureAllVlmDataDeleted(
        AbsRscLayerObject<Resource> rscLayerObjectRef,
        VolumeNumber volumeNumberRef
    )
        throws ImplementationError
    {
        VlmProviderObject<Resource> vlmData = rscLayerObjectRef.getVlmProviderObject(volumeNumberRef);
        if (!vlmData.getRscLayerObject().hasAnyPreventExecutionWhenDeletingReason() && vlmData.exists())
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
                        // start the backup-shipping-daemons if necessary
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

    private boolean isCloneVolume(Volume vlm) throws AccessDeniedException
    {
        StateFlags<Volume.Flags> vlmFlags = vlm.getFlags();
        return vlmFlags.isSet(wrkCtx, Volume.Flags.CLONING_START) &&
            !vlmFlags.isSet(wrkCtx, Volume.Flags.CLONING_FINISHED);
    }

    protected final Resource getResource(Resource anyRsc, String rscName)
        throws AccessDeniedException, StorageException
    {
        @Nullable Resource rsc;
        try
        {
            ResourceName tmpName = new ResourceName(rscName);
            rsc = anyRsc.getNode().getResource(wrkCtx, tmpName);
            if (rsc == null)
            {
                throw new StorageException("Couldn't find resource: " + rscName);
            }
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError("Couldn't create resource name from: " + rscName, exc);
        }
        return rsc;
    }

    private void processClone(
        Collection<Resource> resourceList,
        List<Resource> rscListNotifyApplied,
        HashMap<Resource, ApiCallRcImpl> failedRscs

    )
        throws ImplementationError
    {
        for (Resource rsc : resourceList)
        {
            try
            {
                final Props rscDfnProps = rsc.getResourceDefinition().getProps(wrkCtx);
                final @Nullable String srcRscName = rscDfnProps.getProp(InternalApiConsts.KEY_CLONED_FROM);
                if (srcRscName != null && anyVlmInCloningState(rsc))
                {
                    final Resource srcRsc = getResource(rsc, srcRscName);
                    final AbsRscLayerObject<Resource> srcRootLayerObj = srcRsc.getLayerData(wrkCtx);

                    Map<VlmProviderObject<Resource>, VlmProviderObject<Resource>> devicesToClone = getDevicesToClone(
                        srcRootLayerObj,
                        rsc.getLayerData(wrkCtx)
                    );

                    startClone(devicesToClone);
                }

                final Iterator<Volume> vlmsIt = rsc.iterateVolumes();
                while (vlmsIt.hasNext())
                {
                    Volume vlm = vlmsIt.next();
                    if (vlm.getFlags().isSet(wrkCtx, Volume.Flags.CLONING_FINISHED))
                    {
                        cloneService.removeClone(
                            vlm.getResourceDefinition().getName(),
                            vlm.getVolumeNumber());
                    }
                }
            }
            catch (StorageException exc)
            {
                handleException(rsc, exc);
            }
            catch (AccessDeniedException | DatabaseException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    private boolean anyVlmInCloningState(Resource rscRef) throws AccessDeniedException
    {
        boolean ret = false;
        final Iterator<Volume> vlmsIt = rscRef.iterateVolumes();
        while (vlmsIt.hasNext())
        {
            Volume vlm = vlmsIt.next();
            if (isCloneVolume(vlm))
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    private <SRC_RSC_TYPE extends AbsResource<SRC_RSC_TYPE>>
                Map<VlmProviderObject<SRC_RSC_TYPE>, VlmProviderObject<Resource>> getDevicesToClone(
        AbsRscLayerObject<SRC_RSC_TYPE> sourceLayerDataRef,
        AbsRscLayerObject<Resource> targetLayerDataRef
    )
        throws StorageException
    {
        final Map<VlmProviderObject<SRC_RSC_TYPE>, VlmProviderObject<Resource>> ret = new HashMap<>();

        final DeviceLayer sourceDevLayer = layerFactory.getDeviceLayer(sourceLayerDataRef.getLayerKind());
        final DeviceLayer targetDevLayer = layerFactory.getDeviceLayer(targetLayerDataRef.getLayerKind());

        final DeviceLayer.CloneSupportResult sourceCloneSupport = sourceDevLayer.getCloneSupport(
            sourceLayerDataRef,
            targetLayerDataRef
        );
        final DeviceLayer.CloneSupportResult targetCloneSupport;
        if (sourceDevLayer.equals(targetDevLayer))
        {
            // no need to recalculate twice
            targetCloneSupport = sourceCloneSupport;
        }
        else
        {
            targetCloneSupport = targetDevLayer.getCloneSupport(sourceLayerDataRef, targetLayerDataRef);
        }

        boolean sourceHasPassthrough = hasPassthrough(sourceCloneSupport, sourceLayerDataRef.getLayerKind());
        boolean targetHasPassthrough = hasPassthrough(targetCloneSupport, targetLayerDataRef.getLayerKind());

        sourceLayerDataRef.setClonePassthroughMode(targetLayerDataRef, sourceHasPassthrough);
        targetLayerDataRef.setClonePassthroughMode(targetLayerDataRef, targetHasPassthrough);

        if (sourceHasPassthrough || targetHasPassthrough)
        {
            // continue with recursion

            // if either (source or target) still wants a passthrough, we can advance that layer-tree downwards.
            // advancing is only done depending on the target layer tree. if the source does not have a corresponding
            // rscLayerSuffic as a child, that source will be skipped (i.e. not cloned). the result will be a missing
            // (i.e. not cloned) device, which should not be a problem, since once the cloning is finished, the storage
            // layer will happily create all missing devices before returning to the upper layers.

            if (!targetHasPassthrough)
            {
                // if targetHasPassthrough is already false, sourceHasPassthrough must be true, otherwise the outer if
                // would have gone into the "else" case, not there
                @Nullable AbsRscLayerObject<SRC_RSC_TYPE> sourceRscDataToAdvance = sourceLayerDataRef.getChildBySuffix(
                    targetLayerDataRef.getResourceNameSuffix()
                );
                // sourceRscDataToAdvance might be null if we are cloning from a storage only into a
                // DRBD-with-ext-metadata targetVlmObj with ".meta" suffix will have no srcVlmObj associated
                // in that case, we can just skip cloning, since the external metadata device (and later also the
                // external metadata) will be created automatically. the controller will still have to set the
                // property KEY_FORCE_INITIAL_SYNC_PERMA, but that is true in all storage --clone-> DRBD cases
                if (sourceRscDataToAdvance != null)
                {
                    ret.putAll(getDevicesToClone(sourceRscDataToAdvance, targetLayerDataRef));
                }
            }
            else
            {
                for (AbsRscLayerObject<Resource> targetChildRscData : targetLayerDataRef.getChildren())
                {
                    final String targetChildRscSuffix = targetChildRscData.getResourceNameSuffix();
                    // we only care about target's child-suffixes
                    if (RscLayerSuffixes.shouldSuffixBeCloned(targetChildRscSuffix))
                    {
                        final @Nullable AbsRscLayerObject<SRC_RSC_TYPE> sourceRscDataToAdvance = sourceHasPassthrough ?
                            sourceLayerDataRef.getChildBySuffix(targetChildRscSuffix) :
                            sourceLayerDataRef;
                        if (sourceRscDataToAdvance != null)
                        {
                            ret.putAll(getDevicesToClone(sourceRscDataToAdvance, targetChildRscData));
                        }
                    }
                }
            }
        }
        else
        {
            // recursion ends. populate the map that should be returned

            for (var entry : targetLayerDataRef.getVlmLayerObjects().entrySet())
            {
                // we only need to populate the entries that the target actually wants (and the source also has).
                // that means that we can safely skip a source ".meta" device, if the target does not need it.
                VolumeNumber vlmNr = entry.getKey();
                VlmProviderObject<Resource> targetVlmData = entry.getValue();

                // it still might be possible that the sourceVlmData does not exist (i.e. when cloning from internal
                // metadata into external metadata, targetVlmData will be ".meta", but there will be no such
                // sourceVlmData
                @Nullable VlmProviderObject<SRC_RSC_TYPE> sourceVlmData = sourceLayerDataRef.getVlmProviderObject(
                    vlmNr
                );
                if (sourceVlmData != null)
                {
                    ret.put(sourceVlmData, targetVlmData);
                }
            }
        }

        return ret;
    }

    private boolean hasPassthrough(CloneSupportResult cloneSupportResultRef, DeviceLayerKind layerKind)
        throws StorageException
    {
        final boolean ret;
        switch (cloneSupportResultRef)
        {
            case FALSE:
                throw new StorageException(
                    String.format("Layer %s does not support cloning", layerKind.name())
                );
            case TRUE:
                ret = false;
                break;
            case PASSTHROUGH:
                ret = true;
                break;
            default:
                throw new ImplementationError(
                    String.format("Unexpected clone support result: %s", cloneSupportResultRef.name())
                );
        }
        return ret;
    }

    private Set<CloneStrategy> fixupCloneStrategyQuirks(
            VlmProviderObject<Resource> sourceVlmData,
            VlmProviderObject<Resource> targetVlmData,
            Set<CloneStrategy> srcStrat,
            Set<CloneStrategy> tgtStrat
        ) throws AccessDeniedException
    {
        if (srcStrat.contains(CloneStrategy.ZFS_COPY) && tgtStrat.contains(CloneStrategy.ZFS_COPY))
        {
            String useZfsClone = targetVlmData.getRscLayerObject().getAbsResource().getResourceDefinition()
                .getProps(wrkCtx).getProp(InternalApiConsts.KEY_USE_ZFS_CLONE);
            // zfs clone only works if on the same storage pool
            if (StringUtils.propTrueOrYes(useZfsClone) &&
                sourceVlmData.getStorPool().equals(targetVlmData.getStorPool()))
            {
                srcStrat.add(CloneStrategy.ZFS_CLONE);
                tgtStrat.add(CloneStrategy.ZFS_CLONE);
            }
        }

        // if src and target volume not on the same pool, we can't use LVM_THIN_CLONE
        if (srcStrat.contains(CloneStrategy.LVM_THIN_CLONE) &&
            tgtStrat.contains(CloneStrategy.LVM_THIN_CLONE))
        {
            if (!sourceVlmData.getStorPool().equals(targetVlmData.getStorPool()))
            {
                srcStrat.remove(CloneStrategy.LVM_THIN_CLONE);
                tgtStrat.remove(CloneStrategy.LVM_THIN_CLONE);
            }
        }

        // lowest prio-number first (DD strategy has highest number)
        Set<DeviceHandler.CloneStrategy> resultStrat = new TreeSet<>(
            Comparator.comparingInt(CloneStrategy::getPriority)
        );
        resultStrat.addAll(srcStrat);
        resultStrat.retainAll(tgtStrat);

        return resultStrat;
    }

    private void startClone(Map<VlmProviderObject<Resource>, VlmProviderObject<Resource>> devicesToCloneRef)
        throws AccessDeniedException, DatabaseException
    {
        for (var cloneEntry : devicesToCloneRef.entrySet())
        {
            final VlmProviderObject<Resource> sourceVlmData = cloneEntry.getKey();
            final VlmProviderObject<Resource> targetVlmData = cloneEntry.getValue();
            final Volume targetVlm = (Volume) targetVlmData.getVolume();
            final AbsRscLayerObject<Resource> targetRscData = targetVlmData.getRscLayerObject();
            try
            {
                final VolumeNumber vlmNr = targetVlm.getVolumeNumber();

                final boolean isCloneResource = isCloneVolume(targetVlm);
                if (isCloneResource && !cloneService.isRunning(
                    targetRscData.getResourceName(),
                    vlmNr,
                    targetRscData.getResourceNameSuffix()
                ))
                {
                    Set<DeviceHandler.CloneStrategy> srcStrat = getCloneStrategy(sourceVlmData);
                    Set<DeviceHandler.CloneStrategy> tgtStrat = getCloneStrategy(targetVlmData);

                    Set<DeviceHandler.CloneStrategy> resultStrat = fixupCloneStrategyQuirks(
                        sourceVlmData, targetVlmData, srcStrat, tgtStrat);

                    if (resultStrat.isEmpty())
                    {
                        throw new StorageException(
                            String.format("Failed to clone, no common clone strategy found between:\n" +
                                "  source: %s/%d/%s\n target: %s/%d/%s",
                                sourceVlmData.getRscLayerObject().getSuffixedResourceName(),
                                vlmNr.value,
                                sourceVlmData.getLayerKind().name(),
                                targetRscData.getSuffixedResourceName(),
                                vlmNr.value,
                                targetRscData.getLayerKind().name())
                        );
                    }
                    CloneStrategy cloneStrategy = resultStrat.iterator().next();

                    if (cloneStrategy.needsOpenDevices())
                    {
                        openForClone(sourceVlmData, targetVlmData.getRscLayerObject().getResourceName().displayValue);
                        openForClone(targetVlmData, null);
                    }

                    cloneService.startClone(sourceVlmData, targetVlmData, cloneStrategy);
                }
            }
            catch (StorageException exc)
            {
                cloneService.setFailed(
                    targetVlmData.getRscLayerObject().getResourceName(),
                    targetVlmData.getVlmNr(),
                    targetRscData.getResourceNameSuffix());
                cloneService.notifyCloneStatus(
                    targetVlmData.getRscLayerObject().getResourceName(), targetVlmData.getVlmNr(), true);
                handleException(targetVlmData.getRscLayerObject().getAbsResource(), exc);
            }
        }
    }

    private void notifyResourcesApplied(List<Resource> rscListNotifyApplied) throws ImplementationError
    {
        Peer ctrlPeer = controllerPeerConnector.getControllerPeer();
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
                    .notifyResourceApplied(rsc)
                    .build(),
                InternalApiConsts.API_NOTIFY_RSC_APPLIED
            );
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
        throws AccessDeniedException
    {
        Set<AbsRscLayerObject<RSC>> ret = new HashSet<>();
        for (AbsRscLayerObject<RSC> data : dataSetRef)
        {
            Set<LayerIgnoreReason> ignoreReasons = data.getIgnoreReasons();
            boolean processRscData = shouldRscDataBeProcessed(data, isAbsRscDeleteFlagSet(data));
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

        if (shouldRscDataBeProcessed(rscLayerData, isAbsRscDeleteFlagSet(rscLayerData)))
        {
            errorReporter.logTrace(
                "Layer '%s' processing %s",
                currentLayer.getName(),
                dataDescrFct.apply(rscLayerData)
            );
            try
            {
                processedChildren = procFctRef.process(currentLayer, rscLayerData, apiCallRc);
            }
            catch (StorageException | ResourceException | VolumeException exc)
            {
                if (!shouldExceptionBeIgnored(rscLayerData))
                {
                    throw exc;
                }
                errorReporter.logTrace(
                    "Suppressing exception due to ignore reasons. Exception message: %s",
                    exc.getMessage()
                );
                for (VlmProviderObject<RSC> vlmData : rscLayerData.getVlmLayerObjects().values())
                {
                    vlmData.setExists(false);
                }
            }
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
                "Layer '%s' did not decide if children of '%s' should be processed (ignore reason: '%s'). " +
                    "DeviceHandler proceeds processing children.",
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
            if (!vlmRef.getFlags().isSet(wrkCtx, Volume.Flags.CLONING) &&
                !vlmRef.getFlags().isSet(wrkCtx, Volume.Flags.DRBD_DELETE))
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
                            errorReporter.reportError(exc1);
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


    public Set<DeviceHandler.CloneStrategy> getCloneStrategy(VlmProviderObject<?> vlmProvObj) throws StorageException
    {
        return layerFactory.getDeviceLayer(vlmProvObj.getRscLayerObject().getLayerKind())
            .getCloneStrategy(vlmProvObj);
    }

    @Override
    public void openForClone(
        VlmProviderObject<?> vlmData,
        @Nullable String targetRscNameRef
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        final AbsRscLayerObject<?> rscData = vlmData.getRscLayerObject();
        final DeviceLayer curLayer = layerFactory.getDeviceLayer(rscData.getLayerKind());

        @Nullable Boolean isPassthrough = rscData.isClonePassthroughMode();
        if (isPassthrough != null && isPassthrough)
        {
            final String rscNameSuffix = rscData.getResourceNameSuffix();
            final AbsRscLayerObject<?> childRscData = rscData.getChildBySuffix(rscNameSuffix);
            final VlmProviderObject<?> childVlmData = childRscData.getVlmProviderObject(vlmData.getVlmNr());
            openForClone(childVlmData, targetRscNameRef);
            vlmData.setCloneDevicePath(childVlmData.getCloneDevicePath());
        }
        else
        {
            curLayer.openDeviceForClone(vlmData, targetRscNameRef);
        }
    }

    @Override
    public void closeAfterClone(VlmProviderObject<?> vlmData, @Nullable String targetRscNameRef) throws StorageException
    {
        final AbsRscLayerObject<?> rscData = vlmData.getRscLayerObject();
        final String rscNameSuffix = rscData.getResourceNameSuffix();

        DeviceLayer nextLayer = layerFactory.getDeviceLayer(vlmData.getLayerKind());
        @Nullable Boolean isPassthrough = rscData.isClonePassthroughMode();
        if (isPassthrough != null && isPassthrough)
        {
            AbsRscLayerObject<?> childRscData = rscData.getChildBySuffix(rscNameSuffix);
            VlmProviderObject<?> childVlmData = childRscData.getVlmProviderObject(vlmData.getVlmNr());
            closeAfterClone(childVlmData, targetRscNameRef);
        }
        else
        {
            nextLayer.closeDeviceForClone(vlmData, targetRscNameRef);
        }
    }

    @Override
    public void processAfterClone(VlmProviderObject<?> vlmSrcData, VlmProviderObject<?> vlmTgtData, String clonedDevPath)
        throws StorageException
    {
        final AbsRscLayerObject<?> rscData = vlmTgtData.getRscLayerObject();
        final DeviceLayer curLayer = layerFactory.getDeviceLayer(rscData.getLayerKind());

        curLayer.processAfterClone(vlmSrcData, vlmTgtData, clonedDevPath);

        final String rscNameSuffix = rscData.getResourceNameSuffix();
        AbsRscLayerObject<?> childRscData = rscData.getChildBySuffix(rscNameSuffix);
        if (childRscData != null)
        {
            VlmProviderObject<?> childVlmData = childRscData.getVlmProviderObject(vlmTgtData.getVlmNr());
            processAfterClone(vlmSrcData, childVlmData, clonedDevPath);
        }
    }
}
