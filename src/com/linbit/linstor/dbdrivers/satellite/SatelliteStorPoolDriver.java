package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.linstor.Node;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import javax.inject.Inject;

public class SatelliteStorPoolDriver implements StorPoolDataDatabaseDriver
{
    private final AccessContext dbCtx;

    @Inject
    public SatelliteStorPoolDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @Override
    public StorPoolData load(
        Node node,
        StorPoolDefinition storPoolDefinition,
        boolean logWarnIfNotExists
    )
    {
        StorPoolData storPool = null;
        try
        {
            storPool = (StorPoolData) node.getStorPool(dbCtx, storPoolDefinition.getName());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            SatelliteDbDriverExceptionHandler.handleAccessDeniedException(accDeniedExc);
        }
        return storPool;
    }

    @Override
    public void create(StorPoolData storPoolData)
    {
        // no-op
    }

    @Override
    public void delete(StorPoolData data)
    {
        // no-op
    }

    @Override
    public void ensureEntryExists(StorPoolData data)
    {
        // no-op
    }
}
