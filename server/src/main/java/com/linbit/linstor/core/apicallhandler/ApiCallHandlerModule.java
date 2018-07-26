package com.linbit.linstor.core.apicallhandler;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.serializer.ProtoCommonSerializer;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlStltSerializer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;

import javax.inject.Singleton;

public class ApiCallHandlerModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(CommonSerializer.class).to(ProtoCommonSerializer.class);
        bind(CtrlStltSerializer.class).to(ProtoCtrlStltSerializer.class);
    }

    @Provides
    @Singleton
    @ApiContext
    public AccessContext apiCtx(@SystemContext AccessContext initCtx)
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
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(
                "Could not create API handler's access context",
                accDeniedExc
            );
        }
        return apiCtx;
    }
}
