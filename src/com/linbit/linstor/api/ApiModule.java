package com.linbit.linstor.api;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import com.linbit.linstor.annotation.ApiCallScoped;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.protobuf.ApiCallDescriptor;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

import java.util.List;

public class ApiModule extends AbstractModule
{
    public static final String MSG_ID = "msgId";

    private final ApiType apiType;
    private final List<Class<? extends ApiCall>> apiCalls;

    public ApiModule(
        ApiType apiTypeRef,
        List<Class<? extends ApiCall>> apiCallsRef
    )
    {
        apiType = apiTypeRef;
        apiCalls = apiCallsRef;
    }

    @Override
    protected void configure()
    {
        bind(ApiType.class).toInstance(apiType);

        MapBinder<String, ApiCall> apiCallBinder =
            MapBinder.newMapBinder(binder(), String.class, ApiCall.class);
        MapBinder<String, ApiCallDescriptor> apiCallDescriptorBinder =
            MapBinder.newMapBinder(binder(), String.class, ApiCallDescriptor.class);
        for (Class<? extends ApiCall> apiCall : apiCalls)
        {
            ApiCallDescriptor descriptor = new ApiCallDescriptor(apiType, apiCall);
            apiCallBinder.addBinding(descriptor.getName()).to(apiCall);
            apiCallDescriptorBinder.addBinding(descriptor.getName()).toInstance(descriptor);
        }

        ApiCallScope apiCallScope = new ApiCallScope();
        bindScope(ApiCallScoped.class, apiCallScope);
        bind(ApiCallScope.class).toInstance(apiCallScope);

        bind(AccessContext.class)
            .annotatedWith(PeerContext.class)
            .toProvider(ApiCallScope.<AccessContext>seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(Peer.class)
            .toProvider(ApiCallScope.<Peer>seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(Message.class)
            .toProvider(ApiCallScope.<Message>seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(Integer.class)
            .annotatedWith(Names.named(ApiModule.MSG_ID))
            .toProvider(ApiCallScope.<Integer>seededKeyProvider())
            .in(ApiCallScoped.class);
    }
}
