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
import com.linbit.linstor.storage.layer.ResourceLayer;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.DrbdAdm;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.utils.RemoveAfterDevMgrRework;

import javax.inject.Provider;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DeviceHandlerImpl implements DeviceHandler2
{
    private final AccessContext wrkCtx;
    private final ErrorReporter errorReporter;
    private final DeviceManager deviceManager;

    private final LayeredResourcesHelper layeredRscHelper;
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
            controllerPeerConnectorRef,
            this
        );

        fullSyncApplied = new AtomicBoolean(false);
    }

    @Override
    public void dispatchResources(Collection<Resource> rscs, Collection<Snapshot> snapshots)
    {
        Collection<Resource> origResources = rscs;
        List<Resource> allResources = convertResources(origResources);

        // call prepare for every necessary layer
        Set<Resource> rootResources = origResources.stream().map(rsc -> getRoot(rsc)).collect(Collectors.toSet());
        Map<ResourceLayer, List<Resource>> rscByLayer = allResources.stream()
            .collect(
                Collectors.groupingBy(
                    rsc -> layerFactory.getDeviceLayer(rsc.getType().getDevLayerKind().getClass())
                )
            );
        for (Entry<ResourceLayer, List<Resource>> entry : rscByLayer.entrySet())
        {
            ResourceLayer layer = entry.getKey();
            prepare(layer, entry.getValue());
        }

        // actually process every resource
        for (Resource rsc : rootResources)
        {
            ResourceName rscName = rsc.getDefinition().getName();
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            try
            {
                process(
                    rsc,
                    snapshots.stream()
                        .filter(snap -> snap.getResourceName().equals(rscName))
                        .collect(Collectors.toList()
                    ),
                    apiCallRc
                );
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
            deviceManager.notifyResourceDispatchResponse(rscName, apiCallRc);
        }

        // call clear cache for every layer where the .prepare was called
        for (Entry<ResourceLayer, List<com.linbit.linstor.Resource>> entry : rscByLayer.entrySet())
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
                     deviceManager.notifyResourceDispatchResponse(
                         rsc.getDefinition().getName(),
                         apiCallRc
                     );
                 }
             }
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

    private void prepare(ResourceLayer layer, List<Resource> resources)
    {
        try
        {
            layer.prepare(resources);
        }
        catch (StorageException exc)
        {
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
                deviceManager.notifyResourceDispatchResponse(
                    failedResource.getDefinition().getName(),
                    apiCallRc
                );
            }
        }
        catch (AccessDeniedException | SQLException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public void process(Resource rsc, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException,
            AccessDeniedException, SQLException
    {
        layerFactory
            .getDeviceLayer(
                rsc.getType().getDevLayerKind().getClass()
            )
            .process(rsc, snapshots, apiCallRc);
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
