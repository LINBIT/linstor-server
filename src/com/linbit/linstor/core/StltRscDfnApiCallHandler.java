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
    private Satellite satelliteRef;
    private AccessContext apiCtx;

    public StltRscDfnApiCallHandler(Satellite satelliteRef, AccessContext apiCtx)
    {
        this.satelliteRef = satelliteRef;
        this.apiCtx = apiCtx;
    }

    public void primaryResource(
        String rscNameStr,
        UUID rscUuid
    )
    {
        satelliteRef.getErrorReporter().logInfo("Primary Resource %s", rscNameStr);
        try {
            ResourceName rscName = new ResourceName(rscNameStr);

            ResourceDefinition rscDfn = satelliteRef.rscDfnMap.get(rscName);
            if (rscDfn != null)
            {
                // set primary boolean
                ResourceData rscData = (ResourceData)rscDfn.getResource(this.apiCtx, satelliteRef.getLocalNode().getName());
                rscData.setCreatePrimary();
                satelliteRef.getErrorReporter().logInfo("Primary bool set on Resource %s", rscNameStr);

                satelliteRef.getDeviceManager().getUpdateTracker().checkResource(rscUuid, rscName);
            }
        } catch (InvalidNameException _ignore) {
        } catch (AccessDeniedException accExc) {
            satelliteRef.getErrorReporter().reportError(
                Level.ERROR,
                new ImplementationError(
                    "Worker access context not authorized to perform a required operation",
                    accExc
                )
            );
        }
    }
}

