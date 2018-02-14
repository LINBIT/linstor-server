package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import java.util.UUID;
import org.slf4j.event.Level;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
class StltRscDfnApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final DeviceManager deviceManager;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;

    @Inject
    StltRscDfnApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        rscDfnMap = rscDfnMapRef;
    }

    public void primaryResource(
        String rscNameStr,
        UUID rscUuid
    )
    {
        errorReporter.logInfo("Primary Resource %s", rscNameStr);
        try
        {
            ResourceName rscName = new ResourceName(rscNameStr);

            ResourceDefinition rscDfn = rscDfnMap.get(rscName);
            if (rscDfn != null)
            {
                // set primary boolean
                ResourceData rscData = (ResourceData) rscDfn.getResource(
                    this.apiCtx,
                    controllerPeerConnector.getLocalNode().getName()
                );
                rscData.setCreatePrimary();
                errorReporter.logInfo("Primary bool set on Resource %s", rscNameStr);

                deviceManager.getUpdateTracker().checkResource(rscUuid, rscName);
            }
        }
        catch (InvalidNameException ignored)
        {
        }
        catch (AccessDeniedException accExc)
        {
            errorReporter.reportError(
                Level.ERROR,
                new ImplementationError(
                    "Worker access context not authorized to perform a required operation",
                    accExc
                )
            );
        }
    }
}

