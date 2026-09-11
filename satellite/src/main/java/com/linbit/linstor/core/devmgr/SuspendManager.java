package com.linbit.linstor.core.devmgr;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.LayerFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

@Singleton
public class SuspendManager
{
    private final ErrorReporter errorReporter;
    private @Nullable ExceptionHandler excHandler;
    private final LayerFactory layerFactory;

    @Inject
    public SuspendManager(
        ErrorReporter errorReporterRef,
        LayerFactory layerFactoryRef
    )
    {
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
         *
         * Resumes have to run in the opposite order, non-root layers before their root layer. Resuming the root
         * layer while a lower layer is still suspended can block in the kernel forever, for example DRBD's
         * resume-io gets stuck if DRBD needs to write metadata (i.e. from a resync that finished during the
         * suspend) to its backing LUKS device that is still suspended.
         */
        if (!resumeOnlyRef)
        {
            errorReporter.logTrace("Checking required changes in suspend-io state...");
        }

        List<Resource> suspendedResources = new ArrayList<>();
        List<Resource> resourcesToResume = new ArrayList<>();
        // root-layer only run, only executing suspends. resources needing a resume are just collected here
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
                    rsc.getLayerData(),
                    resumeOnlyRef,
                    true
                );
                switch (suspendIoResult)
                {
                    case SUSPENDED:
                        suspendedResources.add(rsc);
                        break;
                    case RESUME_NEEDED:
                        resourcesToResume.add(rsc);
                        break;
                    case RESUMED: // fall-through
                    case NOOP: // fall-through
                    default:
                        // noop
                        break;
                }
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
                    rsc.getLayerData(),
                    resumeOnlyRef,
                    false
                );
            }
            catch (StorageException exc)
            {
                failedRscs.put(rsc, excHandler.handleException(rsc, exc));
            }
        }

        // resume run, bottom-up: non-root layers have to be resumed before their parents, the root layer last
        for (Resource rsc : resourcesToResume)
        {
            try
            {
                resumeIoBottomUp(rsc.getLayerData(), resumeOnlyRef);
            }
            catch (StorageException exc)
            {
                failedRscs.put(rsc, excHandler.handleException(rsc, exc));
            }
        }
        return failedRscs;
    }

    /**
     * Resumes the given layer tree bottom-up, resuming children before their parents.
     */
    private void resumeIoBottomUp(AbsRscLayerObject<Resource> rscLayerObjectRef, boolean resumeOnlyRef)
        throws StorageException
    {
        if (isManageSuspendNeeded(rscLayerObjectRef))
        {
            for (AbsRscLayerObject<Resource> child : rscLayerObjectRef.getChildren())
            {
                resumeIoBottomUp(child, resumeOnlyRef);
            }
            DeviceLayer layer = layerFactory.getDeviceLayer(rscLayerObjectRef.getLayerKind());
            if (layer.isSuspendIoSupported())
            {
                manageSuspendIo(
                    layer,
                    rscLayerObjectRef,
                    resumeOnlyRef,
                    rscLayerObjectRef.getParent() == null,
                    false
                );
            }
        }
    }

    private ManageSuspendIoResult manageSuspendIoIfNeeded(
        AbsRscLayerObject<Resource> rscLayerObjectRef,
        boolean resumeOnlyRef,
        boolean rootOnlyRef
    )
        throws StorageException
    {
        ManageSuspendIoResult result = ManageSuspendIoResult.NOOP;
        if (isManageSuspendNeeded(rscLayerObjectRef))
        {
            DeviceLayer layer = layerFactory.getDeviceLayer(rscLayerObjectRef.getLayerKind());
            boolean isRootRscData = rscLayerObjectRef.getParent() == null;

            boolean runManageSuspend = layer.isSuspendIoSupported() && (!rootOnlyRef || isRootRscData);
            if (runManageSuspend)
            {
                // during the root-only run resumes are deferred so that resumeIoBottomUp can execute them
                // in the correct (bottom-up) order
                result = manageSuspendIo(layer, rscLayerObjectRef, resumeOnlyRef, isRootRscData, rootOnlyRef);
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
        boolean asRootLayerRef,
        boolean deferResumeRef
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
                    layer.suspendIo(rscData, asRootLayerRef);
                    result = ManageSuspendIoResult.SUSPENDED;
                    rscData.setIsSuspended(true);
                }
                else if (deferResumeRef)
                {
                    result = ManageSuspendIoResult.RESUME_NEEDED;
                }
                else
                {
                    layer.resumeIo(rscData, asRootLayerRef);
                    result = ManageSuspendIoResult.RESUMED;
                    rscData.setIsSuspended(false);
                }
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
        catch (DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        return result;
    }

    private boolean isManageSuspendNeeded(AbsRscLayerObject<Resource> rscDataRef)
    {
        StateFlags<Flags> flags = rscDataRef.getAbsResource().getStateFlags();
        boolean isRscInactive = flags.isSet(
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
        NOOP, SUSPENDED, RESUMED, RESUME_NEEDED;
    }
}
