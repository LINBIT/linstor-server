package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import java.util.UUID;
import org.slf4j.event.Level;

class StltRscDfnApiCallHandler
{
    private Satellite satellite;
    private AccessContext apiCtx;

    StltRscDfnApiCallHandler(Satellite satelliteRef, AccessContext apiCtxRef)
    {
        satellite = satelliteRef;
        apiCtx = apiCtxRef;
    }

    public void primaryResource(
        String rscNameStr,
        UUID rscUuid
    )
    {
        satellite.getErrorReporter().logInfo("Primary Resource %s", rscNameStr);
        try
        {
            ResourceName rscName = new ResourceName(rscNameStr);

            ResourceDefinition rscDfn = satellite.rscDfnMap.get(rscName);
            if (rscDfn != null)
            {
                // set primary boolean
                ResourceData rscData = (ResourceData) rscDfn.getResource(
                    this.apiCtx,
                    satellite.getLocalNode().getName()
                );
                rscData.setCreatePrimary();
                satellite.getErrorReporter().logInfo("Primary bool set on Resource %s", rscNameStr);

                satellite.getDeviceManager().getUpdateTracker().checkResource(rscUuid, rscName);
            }
        }
        catch (InvalidNameException ignored)
        {
        }
        catch (AccessDeniedException accExc)
        {
            satellite.getErrorReporter().reportError(
                Level.ERROR,
                new ImplementationError(
                    "Worker access context not authorized to perform a required operation",
                    accExc
                )
            );
        }
    }
}

