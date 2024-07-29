package com.linbit.linstor.core.devmgr;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.LayerFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

@Singleton
public class SuspendManager
{
    private final ErrorReporter errorReporter;
    private final AccessContext wrkCtx;
    private @Nullable ExceptionHandler excHandler;
    private final LayerFactory layerFactory;

    @Inject
    public SuspendManager(
        @DeviceManagerContext AccessContext wrkCtxRef,
        ErrorReporter errorReporterRef,
        LayerFactory layerFactoryRef
    )
    {
        wrkCtx = wrkCtxRef;
        errorReporter = errorReporterRef;
        layerFactory = layerFactoryRef;
    }

    HashMap<Resource, ApiCallRcImpl> manageSuspendIo(Collection<Resource> rscsRef, boolean resumeOnlyRef)
    {
        HashMap<Resource, ApiCallRcImpl> failedRscs = new HashMap<>();
        /*
         * We want to run all suspend-io as soon and as close as possible to each other on the root layers.
         * resume-io's are not that important to be close together, so we only focus on non-suspended -> suspended
         * transitions.
         *
         * After the root layer's are suspended we also want to give the non-root layers a chance (opt-in) to
         * also perform some suspend commands (needed for setups like DRBD,WRITECACHE,STORAGE such that writecache
         * can flush its cache before a snapshot is taken)
         */
        if (!resumeOnlyRef)
        {
            errorReporter.logTrace("Checking required changes in suspend-io state...");
        }

        List<Resource> suspendedResources = new ArrayList<>();
        // root-layer only run
        for (Resource rsc : rscsRef)
        {
            try
            {
                /*
                 * TODO this step could be parallelized so that all resources can run their suspend commands
                 * as close to each other as possible without having to wait for the previous to finish. This
                 * is recommended for setups where writecache is the root layer of multiple resources, and a flush
                 * is expected to take quite some time
                 */
                ManageSuspendIoResult suspendIoResult = manageSuspendIoIfNeeded(
                    rsc.getLayerData(wrkCtx),
                    resumeOnlyRef
                );
                switch (suspendIoResult)
                {
                    case SUSPENDED:
                        suspendedResources.add(rsc);
                        break;
                    case NOOP: // fall-through
                    case RESUMED: // fall-through
                    default:
                        // noop
                        break;
                }
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (StorageException exc)
            {
                failedRscs.put(rsc, excHandler.handleException(rsc, exc));
            }
        }

        // non-root-layer post-suspend run
        for (Resource rsc : suspendedResources)
        {
            try
            {
                // since the root layers are already suspended, there is no need to parallelize this step
                AbsRscLayerObject<Resource> rootData = rsc.getLayerData(wrkCtx);

                managePostRootSuspendIoIfNeededRec(rootData.getChildren());
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (StorageException | ResourceException exc)
            {
                failedRscs.put(rsc, excHandler.handleException(rsc, exc));
            }
        }

        return failedRscs;
    }

    private void managePostRootSuspendIoIfNeededRec(Set<AbsRscLayerObject<Resource>> childrenRef)
        throws ResourceException, StorageException, AccessDeniedException
    {
        if (childrenRef != null)
        {
            for (AbsRscLayerObject<Resource> child : childrenRef)
            {
                DeviceLayer layer = layerFactory.getDeviceLayer(child.getLayerKind());
                layer.managePostRootSuspend(child);

                managePostRootSuspendIoIfNeededRec(child.getChildren());
            }
        }
    }

    private ManageSuspendIoResult manageSuspendIoIfNeeded(
        AbsRscLayerObject<Resource> rscLayerObjectRef,
        boolean resumeOnlyRef
    )
        throws StorageException, AccessDeniedException
    {
        ManageSuspendIoResult result = ManageSuspendIoResult.NOOP;
        if (isManageSuspendNeeded(rscLayerObjectRef))
        {
            DeviceLayer layer = layerFactory.getDeviceLayer(rscLayerObjectRef.getLayerKind());
            boolean isRootRscData = rscLayerObjectRef.getParent() == null;

            boolean runManageSuspend = layer.isSuspendIoSupported() && isRootRscData;
            if (runManageSuspend)
            {
                result = manageSuspendIo(layer, rscLayerObjectRef, resumeOnlyRef);
            }
        }
        return result;
    }

    private ManageSuspendIoResult manageSuspendIo(
        DeviceLayer layer,
        AbsRscLayerObject<Resource> rscData,
        boolean resumeOnlyRef
    )
        throws StorageException
    {
        ManageSuspendIoResult result = ManageSuspendIoResult.NOOP;
        boolean shouldSuspend = rscData.exists() && rscData.getShouldSuspendIo() && !resumeOnlyRef;
        try
        {
            layer.updateSuspendState(rscData);

            boolean isSuspended = rscData.isSuspended() != null && rscData.isSuspended();
            if (isSuspended != shouldSuspend)
            {
                if (shouldSuspend)
                {
                    layer.suspendIo(rscData);
                    result = ManageSuspendIoResult.SUSPENDED;
                }
                else
                {
                    layer.resumeIo(rscData);
                    result = ManageSuspendIoResult.RESUMED;
                }
                rscData.setIsSuspended(shouldSuspend);
            }
        }
        catch (ExtCmdFailedException exc)
        {
            throw new StorageException(
                String.format(
                    "Failed to %s IO for resource %s on layer %s",
                    shouldSuspend ? "suspend" : "resume",
                    rscData.getSuffixedResourceName(),
                    layer.getName()
                ),
                exc
            );
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        return result;
    }

    private boolean isManageSuspendNeeded(AbsRscLayerObject<Resource> rscDataRef) throws AccessDeniedException
    {
        StateFlags<Flags> flags = rscDataRef.getAbsResource().getStateFlags();
        boolean isRscInactive = flags.isSet(
            wrkCtx,
            Resource.Flags.INACTIVE,
            Resource.Flags.INACTIVE_PERMANENTLY,
            Resource.Flags.INACTIVATING
        );
        return rscDataRef.exists() && !isRscInactive;
    }

    void setExceptionHandler(ExceptionHandler excHandlerRef)
    {
        excHandler = excHandlerRef;
    }

    interface ExceptionHandler
    {
        ApiCallRcImpl handleException(Resource rsc, Throwable exc);
    }

    private enum ManageSuspendIoResult
    {
        NOOP, SUSPENDED, RESUMED;
    }
}
