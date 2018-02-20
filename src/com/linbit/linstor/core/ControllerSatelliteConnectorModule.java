package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.linstor.annotation.SatelliteConnectorContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;

import javax.inject.Singleton;

public class ControllerSatelliteConnectorModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(SatelliteConnector.class).to(SatelliteConnectorImpl.class);
    }

    @Provides
    @Singleton
    @SatelliteConnectorContext
    public AccessContext satelliteConnector(@SystemContext AccessContext initCtx)
        throws AccessDeniedException
    {
        AccessContext connectorCtx = initCtx.clone();
        connectorCtx.getEffectivePrivs().enablePrivileges(
            Privilege.PRIV_MAC_OVRD,
            Privilege.PRIV_OBJ_CHANGE
        );
        return connectorCtx;
    }
}
