package com.linbit.linstor.core;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.linbit.linstor.annotation.SatelliteConnectorContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlClientSerializer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.tasks.ReconnectorTask;

import javax.inject.Singleton;

public class CtrlApiCallHandlerModule extends PrivateModule
{
    @Override
    protected void configure()
    {
        bind(CtrlClientSerializer.class).to(ProtoCtrlClientSerializer.class);
        expose(CtrlClientSerializer.class);

        bind(SatelliteConnector.class).to(SatelliteConnectorImpl.class);

        bind(CtrlApiCallHandler.class);
        expose(CtrlApiCallHandler.class);

        // Expose CtrlNodeApiCallHandler to allow connections to be started
        bind(CtrlNodeApiCallHandler.class);
        expose(CtrlNodeApiCallHandler.class);

        bind(ReconnectorTask.class);
        expose(ReconnectorTask.class);
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
