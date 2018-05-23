package com.linbit.linstor.event.writer.protobuf.controller;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiRcUtils;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.generator.ResourceDeploymentStateGenerator;
import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.event.writer.protobuf.ProtobufEventWriter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.locks.ReadWriteLock;

@ProtobufEventWriter(
    eventName = ApiConsts.EVENT_SNAPSHOT_DEPLOYMENT,
    objectType = WatchableObject.SNAPSHOT_DEFINITION
)
@Singleton
public class SnapshotDeploymentEvent implements EventWriter
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ReadWriteLock rscDfnMapLock;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final CtrlClientSerializer ctrlClientSerializer;
    private final ResourceDeploymentStateGenerator resourceDeploymentStateGenerator;

    @Inject
    public SnapshotDeploymentEvent(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CtrlClientSerializer ctrlClientSerializerRef,
        ResourceDeploymentStateGenerator resourceDeploymentStateGeneratorRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        rscDfnMapLock = rscDfnMapLockRef;
        rscDfnMap = rscDfnMapRef;
        ctrlClientSerializer = ctrlClientSerializerRef;
        resourceDeploymentStateGenerator = resourceDeploymentStateGeneratorRef;
    }

    @Override
    public byte[] writeEvent(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        ApiCallRc apiCallRc;

        rscDfnMapLock.readLock().lock();
        try
        {
            ResourceDefinition rscDfn = rscDfnMap.get(objectIdentifier.getResourceName());

            if (rscDfn == null)
            {
                errorReporter.logWarning("Unable to write snapshot deployment event for unknown resource '%s'",
                    objectIdentifier.getResourceName());
                apiCallRc = null;
            }
            else
            {
                SnapshotDefinition snapshotDfn = rscDfn.getSnapshotDfn(apiCtx, objectIdentifier.getSnapshotName());

                if (snapshotDfn == null)
                {
                    errorReporter.logWarning(
                        "Unable to write snapshot deployment event for unknown snapshot '%s' of resource '%s'",
                        objectIdentifier.getSnapshotName(), rscDfn.getName());
                    apiCallRc = null;
                }
                else
                {
                    apiCallRc = determineResponse(rscDfn, snapshotDfn);
                }
            }
        }
        finally
        {
            rscDfnMapLock.readLock().unlock();
        }

        return ctrlClientSerializer.builder().snapshotDeploymentEvent(apiCallRc).build();
    }

    private ApiCallRc determineResponse(ResourceDefinition rscDfn, SnapshotDefinition snapshotDfn)
        throws Exception
    {
        ApiCallRc apiCallRc;

        if (snapshotDfn.getFlags().isSet(apiCtx, SnapshotDefinition.SnapshotDfnFlags.FAILED_DEPLOYMENT))
        {
            ApiCallRcImpl combinedDeploymentRc = new ApiCallRcImpl();
            for (Snapshot snapshot : snapshotDfn.getAllSnapshots())
            {
                ApiCallRc deploymentRc = resourceDeploymentStateGenerator.generate(new ObjectIdentifier(
                    snapshot.getNode().getName(),
                    rscDfn.getName(),
                    null,
                    snapshotDfn.getName()
                ));
                if (deploymentRc != null && ApiRcUtils.isError(deploymentRc))
                {
                    for (ApiCallRc.RcEntry rcEntry : deploymentRc.getEntries())
                    {
                        combinedDeploymentRc.addEntry(rcEntry);
                    }
                }
            }

            apiCallRc = combinedDeploymentRc;
        }
        else if (snapshotDfn.getFlags().isSet(apiCtx, SnapshotDefinition.SnapshotDfnFlags.SUCCESSFUL))
        {
            ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcImpl.ApiCallRcEntry();
            entry.setReturnCode(ApiConsts.CREATED | ApiConsts.MASK_SNAPSHOT);
            entry.setMessageFormat(String.format("Snapshot '%s' of resource '%s' successfully taken.",
                snapshotDfn.getName().displayValue,
                rscDfn.getName().displayValue));
            ApiCallRcImpl successRc = new ApiCallRcImpl();
            successRc.addEntry(entry);

            apiCallRc = successRc;
        }
        else if (snapshotDfn.getFlags().isSet(apiCtx, SnapshotDefinition.SnapshotDfnFlags.FAILED_DISCONNECT))
        {
            ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcImpl.ApiCallRcEntry();
            entry.setReturnCode(ApiConsts.FAIL_NOT_CONNECTED);
            entry.setMessageFormat(String.format(
                "Snapshot '%s' of resource '%s' failed due to satellite disconnection.",
                snapshotDfn.getName().displayValue,
                rscDfn.getName().displayValue
            ));
            ApiCallRcImpl failedRc = new ApiCallRcImpl();
            failedRc.addEntry(entry);

            apiCallRc = failedRc;
        }
        else
        {
            // Still in progress - empty response
            apiCallRc = new ApiCallRcImpl();
        }

        return apiCallRc;
    }

    @Override
    public void clear(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        rscDfnMapLock.readLock().lock();
        try
        {
            ResourceDefinition rscDfn = rscDfnMap.get(objectIdentifier.getResourceName());

            if (rscDfn != null)
            {
                // Snapshot definitions are (currently) only stored for in-progress snapshots
                rscDfn.removeSnapshotDfn(apiCtx, objectIdentifier.getSnapshotName());
            }
        }
        finally
        {
            rscDfnMapLock.readLock().unlock();
        }
    }
}
