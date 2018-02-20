package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.TransactionMgr;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnectionData;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;

public class SatelliteResConDfnDriver implements ResourceConnectionDataDatabaseDriver
{
    private final AccessContext dbCtx;

    @Inject
    public SatelliteResConDfnDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @Override
    public ResourceConnectionData load(
        Resource source,
        Resource target,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )

    {
        ResourceConnectionData resourceConnection = null;
        try
        {
            resourceConnection = (ResourceConnectionData) source.getResourceConnection(dbCtx, target);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            SatelliteDbDriverExceptionHandler.handleAccessDeniedException(accDeniedExc);
        }
        return resourceConnection;
    }

    @Override
    public List<ResourceConnectionData> loadAllByResource(
        Resource resource,
        TransactionMgr transMgr
    )

    {
        return Collections.emptyList();
    }

    @Override
    public void create(ResourceConnectionData conDfnData, TransactionMgr transMgr)
    {
        // no-op
    }

    @Override
    public void delete(ResourceConnectionData data, TransactionMgr transMgr)
    {
        // no-op
    }
}
