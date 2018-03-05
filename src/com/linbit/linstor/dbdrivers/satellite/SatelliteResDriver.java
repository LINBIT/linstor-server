package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.linstor.Node;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import javax.inject.Inject;

public class SatelliteResDriver implements ResourceDataDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final AccessContext dbCtx;

    @Inject
    public SatelliteResDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<ResourceData> getStateFlagPersistence()
    {
        return (StateFlagsPersistence<ResourceData>) stateFlagsDriver;
    }

    @Override
    public void create(ResourceData res)
    {
        // no-op
    }

    @Override
    public ResourceData load(Node node, ResourceName resourceName, boolean logWarnIfNotExists)

    {
        ResourceData resource = null;
        try
        {
            resource = (ResourceData) node.getResource(dbCtx, resourceName);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            SatelliteDbDriverExceptionHandler.handleAccessDeniedException(accDeniedExc);
        }
        return resource;
    }

    @Override
    public void delete(ResourceData resourceData)
    {
        // no-op
    }
}
