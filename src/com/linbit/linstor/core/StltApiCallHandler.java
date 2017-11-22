package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.security.AccessContext;

public class StltApiCallHandler
{
    private Satellite satellite;
    private StltRscApiCallHandler rscHandler;
    private StltStorPoolApiCallHandler storPoolHandler;

    public StltApiCallHandler(Satellite satellite, ApiType apiType, AccessContext apiCtx)
    {
        this.satellite = satellite;
        rscHandler = new StltRscApiCallHandler(satellite, apiCtx);
        storPoolHandler = new StltStorPoolApiCallHandler(satellite, apiCtx);
    }

    public void deployResource(RscPojo rscRawData)
    {
        try
        {
            satellite.reconfigurationLock.writeLock().lock();
            // TODO: acquire nodesMapLock and rscDfnMapLock
            rscHandler.deployResource(rscRawData);
        }
        catch (DivergentDataException | ImplementationError exc)
        {
            satellite.getErrorReporter().reportError(exc);
        }
        finally
        {
            satellite.reconfigurationLock.writeLock().unlock();
        }
    }

    public void deployStorPool(StorPoolPojo storPoolRaw)
    {
        try
        {
            satellite.reconfigurationLock.writeLock().lock();
            // TODO: acquire nodesMapLock and storPoolDfnMapLock
            storPoolHandler.deployStorPool(storPoolRaw);
        }
        catch (DivergentDataException | ImplementationError exc)
        {
            satellite.getErrorReporter().reportError(exc);
        }
        finally
        {
            satellite.reconfigurationLock.writeLock().unlock();
        }
    }

}
