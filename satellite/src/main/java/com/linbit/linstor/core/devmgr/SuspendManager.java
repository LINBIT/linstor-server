package com.linbit.linstor.core.devmgr;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

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
                    resumeOnlyRef,
                    true
                );
                switch (suspendIoResult)
                {
                    case SUSPENDED:
                    case RESUMED: // fall-through
                        suspendedResources.add(rsc);
                        break;
                    case NOOP: // fall-through
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
                manageSuspendIoIfNeeded(
                    rsc.getLayerData(wrkCtx),
                    resumeOnlyRef,
                    false
                );
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
        return failedRscs;
    }

    private ManageSuspendIoResult manageSuspendIoIfNeeded(
        AbsRscLayerObject<Resource> rscLayerObjectRef,
        boolean resumeOnlyRef,
        boolean rootOnlyRef
    )
        throws StorageException, AccessDeniedException
    {
        ManageSuspendIoResult result = ManageSuspendIoResult.NOOP;
        if (isManageSuspendNeeded(rscLayerObjectRef))
        {
            DeviceLayer layer = layerFactory.getDeviceLayer(rscLayerObjectRef.getLayerKind());
            boolean isRootRscData = rscLayerObjectRef.getParent() == null;

            boolean runManageSuspend = layer.isSuspendIoSupported() && (!rootOnlyRef || isRootRscData);
            if (runManageSuspend)
            {
                result = manageSuspendIo(layer, rscLayerObjectRef, resumeOnlyRef, rootOnlyRef);
            }
            if (!rootOnlyRef)
            {
                for (AbsRscLayerObject<Resource> child : rscLayerObjectRef.getChildren())
                {
                    manageSuspendIoIfNeeded(child, resumeOnlyRef, rootOnlyRef);
                }
            }
        }
        return result;
    }

    private ManageSuspendIoResult manageSuspendIo(
        DeviceLayer layer,
        AbsRscLayerObject<Resource> rscData,
        boolean resumeOnlyRef,
        boolean rootOnlyRef
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
                    layer.suspendIo(rscData, rootOnlyRef);
                    result = ManageSuspendIoResult.SUSPENDED;
                }
                else
                {
                    layer.resumeIo(rscData, rootOnlyRef);
                    result = ManageSuspendIoResult.RESUMED;
                }
                rscData.setIsSuspended(shouldSuspend);
            }
        }
        catch (ExtCmdFailedException | ChildProcessTimeoutException | IOException exc)
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
        catch (AccessDeniedException | DatabaseException exc)
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
