package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import java.util.UUID;
import org.slf4j.event.Level;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
class StltRscDfnApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final DeviceManager deviceManager;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    StltRscDfnApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        rscDfnMap = rscDfnMapRef;
        transMgrProvider = transMgrProviderRef;
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
                Resource rscData = (Resource) rscDfn.getResource(
                    this.apiCtx,
                    controllerPeerConnector.getLocalNode().getName()
                );
                rscData.setCreatePrimary();
                errorReporter.logInfo("Primary bool set on Resource %s", rscNameStr);

                deviceManager.markResourceForDispatch(rscName);
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

