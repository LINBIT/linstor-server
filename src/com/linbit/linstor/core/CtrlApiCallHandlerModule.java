package com.linbit.linstor.core;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlClientSerializer;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlStltSerializer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.tasks.ReconnectorTask;

import javax.inject.Named;

public class CtrlApiCallHandlerModule extends PrivateModule
{
    public static final String SATELLITE_CONNECTOR_CTX = "satelliteConnectorCtx";

    private final AccessContext initCtx;

    public CtrlApiCallHandlerModule(AccessContext initCtxRef)
    {
        initCtx = initCtxRef;
    }

    @Override
    protected void configure()
    {
        bind(ApiType.class).toInstance(ApiType.PROTOBUF);

        bind(CtrlStltSerializer.class).to(ProtoCtrlStltSerializer.class);
        bind(CtrlClientSerializer.class).to(ProtoCtrlClientSerializer.class);
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
    public AccessContext apiCtx()
    {
        AccessContext apiCtx = initCtx.clone();
        try
        {
            apiCtx.getEffectivePrivs().enablePrivileges(
                Privilege.PRIV_OBJ_VIEW,
                Privilege.PRIV_OBJ_USE,
                Privilege.PRIV_OBJ_CHANGE,
                Privilege.PRIV_OBJ_CONTROL,
                Privilege.PRIV_MAC_OVRD
            );
            return apiCtx;
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(
                "Could not create API handler's access context",
                accDeniedExc
            );
        }
    }

    @Provides
    @Singleton
    @Named(SATELLITE_CONNECTOR_CTX)
    public AccessContext satelliteConnector()
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
