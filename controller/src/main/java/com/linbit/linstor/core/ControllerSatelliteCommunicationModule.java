package com.linbit.linstor.core;

import com.linbit.linstor.annotation.SatelliteConnectorContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcherProto;
import com.linbit.linstor.core.apicallhandler.controller.VlmAllocatedFetcher;
import com.linbit.linstor.core.apicallhandler.controller.VlmAllocatedFetcherProto;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.PrivilegeSet;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class ControllerSatelliteCommunicationModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(SatelliteConnector.class).to(SatelliteConnectorImpl.class);
        bind(FreeCapacityFetcher.class).to(FreeCapacityFetcherProto.class);
        bind(VlmAllocatedFetcher.class).to(VlmAllocatedFetcherProto.class);
    }

    @Provides
    @Singleton
    @SatelliteConnectorContext
    public AccessContext satelliteConnector(@SystemContext AccessContext initCtx)
        throws AccessDeniedException
    {
        AccessContext connectorCtx = initCtx.clone();
        PrivilegeSet connEffPriv = connectorCtx.getEffectivePrivs();
        connEffPriv.disablePrivileges(Privilege.PRIV_SYS_ALL);
        connEffPriv.enablePrivileges(Privilege.PRIV_MAC_OVRD, Privilege.PRIV_OBJ_CHANGE);
        PrivilegeSet connLimPriv = connectorCtx.getLimitPrivs();
        connLimPriv.disablePrivileges(
            Privilege.PRIV_OBJ_OWNER,
            Privilege.PRIV_OBJ_CONTROL
        );
        return connectorCtx;
    }
}
