package com.linbit.linstor.security;

import com.linbit.linstor.annotation.ApiCallScoped;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class TestApiModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        LinStorScope apiCallScope = new LinStorScope();
        bindScope(ApiCallScoped.class, apiCallScope);
        bind(LinStorScope.class).toInstance(apiCallScope);

        bind(AccessContext.class)
            .annotatedWith(PeerContext.class)
            .toProvider(LinStorScope.<AccessContext>seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(Peer.class)
            .toProvider(LinStorScope.<Peer>seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(Message.class)
            .toProvider(LinStorScope.<Message>seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(Integer.class)
            .annotatedWith(Names.named(ApiModule.API_CALL_ID))
            .toProvider(LinStorScope.<Integer>seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(TransactionMgr.class)
            .toProvider(LinStorScope.<TransactionMgr>seededKeyProvider())
            .in(ApiCallScoped.class);
        bind(TransactionMgrSQL.class)
            .toProvider(LinStorScope.<TransactionMgrSQL>seededKeyProvider())
            .in(ApiCallScoped.class);
    }
}
