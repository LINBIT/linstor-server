package com.linbit.linstor.api;

import com.linbit.linstor.annotation.ApiCallScoped;
import com.linbit.linstor.annotation.ErrorReporterContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.protobuf.ApiCallDescriptor;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;

import java.util.List;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;

public class ApiModule extends AbstractModule
{
    public static final String API_CALL_ID = "apiCallId";
    public static final String API_CALL_NAME = "apiCallName";

    private final ApiType apiType;
    private final List<Class<? extends BaseApiCall>> apiCalls;

    public ApiModule(
        ApiType apiTypeRef,
        List<Class<? extends BaseApiCall>> apiCallsRef
    )
    {
        apiType = apiTypeRef;
        apiCalls = apiCallsRef;
    }

    @Override
    protected void configure()
    {
        bind(ApiType.class).toInstance(apiType);

        MapBinder<String, BaseApiCall> apiCallBinder =
            MapBinder.newMapBinder(binder(), String.class, BaseApiCall.class);
        MapBinder<String, ApiCallDescriptor> apiCallDescriptorBinder =
            MapBinder.newMapBinder(binder(), String.class, ApiCallDescriptor.class);
        for (Class<? extends BaseApiCall> apiCall : apiCalls)
        {
            ApiCallDescriptor descriptor = new ApiCallDescriptor(apiType, apiCall);
            apiCallBinder.addBinding(descriptor.getName()).to(apiCall);
            apiCallDescriptorBinder.addBinding(descriptor.getName()).toInstance(descriptor);
        }

        LinStorScope apiCallScope = new LinStorScope();
        bindScope(ApiCallScoped.class, apiCallScope);
        bind(LinStorScope.class).toInstance(apiCallScope);

        bind(AccessContext.class)
            .annotatedWith(PeerContext.class)
            .toProvider(LinStorScope.seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(AccessContext.class)
            .annotatedWith(ErrorReporterContext.class)
            .toProvider(() -> null) // do not complain about not being in a scope, just return null
            .in(ApiCallScoped.class);
        bind(Peer.class)
            .toProvider(LinStorScope.seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(Message.class)
            .toProvider(LinStorScope.seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(Long.class)
            .annotatedWith(Names.named(ApiModule.API_CALL_ID))
            .toProvider(LinStorScope.seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(TransactionMgr.class)
            .toProvider(LinStorScope.seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(TransactionMgrSQL.class)
            .toProvider(LinStorScope.seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(TransactionMgrK8sCrd.class)
            .toProvider(LinStorScope.seededKeyProvider())
            .in(ApiCallScoped.class);
    }
}
