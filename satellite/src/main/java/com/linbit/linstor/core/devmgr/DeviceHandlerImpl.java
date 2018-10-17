package com.linbit.linstor.core.devmgr;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.devmgr.helper.LayeredResourcesHelper;
import com.linbit.linstor.drbdstate.DrbdStateStore;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.LayerFactory;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.DrbdAdm;
import com.linbit.linstor.storage2.layer.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.utils.Pair;
import com.linbit.utils.RemoveAfterDevMgrRework;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class DeviceHandlerImpl implements DeviceHandler2
{
    private final AccessContext wrkCtx;
    private final ErrorReporter errorReporter;
    private final DeviceManager deviceManager;

    private final LayeredResourcesHelper layeredRscHelper;
    private final TopDownOrder strategyTopDown;
    private final BottomUpOrder strategyBottomUp;
    private final LayerFactory layerFactory;
    private final AtomicBoolean fullSyncApplied;

    public DeviceHandlerImpl(
        AccessContext wrkCtxRef,
        ErrorReporter errorReporterRef,
        DeviceManager deviceManagerRef,
        ExtCmdFactory extCmdFactoryRef,
        StltConfigAccessor stltCfgAccessorRef,
        Provider<TransactionMgr> transMgrProviderRef,
        DrbdAdm drbdUtils,
        DrbdStateStore drbdState,
        WhitelistProps whitelistProps,
        LayeredResourcesHelper layeredRscHelperRef,
        CtrlStltSerializer interComSerializerRef,
        ControllerPeerConnector controllerPeerConnectorRef
    )
    {
        wrkCtx = wrkCtxRef;
        errorReporter = errorReporterRef;
        deviceManager = deviceManagerRef;

        layeredRscHelper = layeredRscHelperRef;

        layerFactory = new LayerFactory(
            wrkCtxRef,
            deviceManagerRef,
            drbdUtils,
            drbdState,
            errorReporterRef,
            whitelistProps,
            extCmdFactoryRef,
            stltCfgAccessorRef,
            transMgrProviderRef,
            interComSerializerRef,
            controllerPeerConnectorRef
        );

        strategyTopDown = new TopDownOrder(wrkCtx);
        strategyBottomUp = new BottomUpOrder(wrkCtx);

        fullSyncApplied = new AtomicBoolean(false);
    }

    @Override
    public void dispatchResource(Collection<Resource> rscs, Collection<Snapshot> snapshots)
    {
        Collection<Resource> origResources = rscs;
        List<Resource> allResources = convertResources(origResources);

        /*
         * Every resource is iterated twice, except the storage resources.
         *
         * This has to be done so that every layer can clean up resources and / or volumes
         * (i.e. delete step) which has to be done in a top-down fashion and afterwards create
         * the new volumes in the second phase.
         */
        Map<Resource, StorageException> exceptions = new TreeMap<>();

        try
        {
            /*
             * TODO: review if the data from snapshots could be stored within resources
             * so that we do not need to iterate both, resources and snapshots every time for every layer
             */
            if (fullSyncApplied.getAndSet(false))
            {
                // on the very first run, we need an additional bottom-up run to get all data
                // from storage upwards. The later runs can be applied to the previous data.
                dispatchResources(exceptions, allResources, snapshots, strategyBottomUp);
            }
            dispatchResources(exceptions, allResources, snapshots, strategyTopDown);
            dispatchResources(exceptions, allResources, snapshots, strategyBottomUp);
        }
        catch (StorageException exc)
        {
            errorReporter.reportError(exc);
        }

        for (StorageException exc : exceptions.values())
        {
            errorReporter.reportError(exc);
        }
    }

    private void dispatchResources(
        Map<Resource, StorageException> exceptions,
        Collection<Resource> rscList,
        Collection<Snapshot> snapshots,
        TraverseOrder traverseOrder
    )
        throws StorageException
    {
        ArrayList<Resource> resourcesToProcess = new ArrayList<>(rscList);

        while (!resourcesToProcess.isEmpty())
        {
            List<Pair<DeviceLayerKind, List<Resource>>> batches = traverseOrder.getAllBatches(resourcesToProcess);
            Collections.sort(
                batches,
                (batch1, batch2) ->
                    Long.compare(
                        // comparing 2 with 1 -> [0] should be the largest
                        traverseOrder.getProcessableCount(batch2.objB, resourcesToProcess),
                        traverseOrder.getProcessableCount(batch1.objB, resourcesToProcess)
                    )
            );
            Pair<DeviceLayerKind, List<Resource>> nextBatch = batches.get(0);

            switch (traverseOrder.getPhase())
            {
                case TOP_DOWN:
                    exceptions.putAll(
                        getDeviceLayer(nextBatch.objA)
                            .adjustTopDown(nextBatch.objB, snapshots)
                    );
                    break;
                case BOTTOM_UP:
                    exceptions.putAll(
                        getDeviceLayer(nextBatch.objA)
                            .adjustBottomUp(nextBatch.objB, snapshots)
                    );
                    break;
                default:
                    throw new ImplementationError("Unknown TraverseOrder-Phase: " + traverseOrder.getPhase().name());
            }

            // TODO: we need to prevent deploying resources which dependency resource failed
            resourcesToProcess.removeAll(nextBatch.objB);

            nextBatch.objB.forEach(
                rsc ->
                {
                    ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                    ResourceName rscName = rsc.getDefinition().getName();
                    apiCallRc.addEntry(ApiCallRcImpl
                        .entryBuilder(ApiConsts.MODIFIED, "Resource deployed: " + rsc)
                        .putObjRef(ApiConsts.KEY_RSC_DFN, rscName.displayValue)
                        .build()
                    );
                    deviceManager.notifyResourceDispatchResponse(rscName, apiCallRc);
                }
            );
        }
    }

    private DeviceLayer getDeviceLayer(DeviceLayerKind kind)
    {
        return layerFactory.getDeviceLayer(kind.getClass());
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

    public void fullSyncApplied()
    {
        fullSyncApplied.set(true);
    }
}
