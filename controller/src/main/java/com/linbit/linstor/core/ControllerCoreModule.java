package com.linbit.linstor.core;

import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.AuthToken;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class ControllerCoreModule extends AbstractModule
{
    /**
     * REST responses (Jersey entity writing + the blocking Grizzly write) run on this scheduler instead of
     * the in-order satellite message worker that completed the satellite call. Bounded: 16 threads, 32 queued
     * responses; a full queue makes publishOn reject, which RequestHelper answers with 503 + Retry-After.
     */
    public static final String REST_RESPONSE_SCHEDULER = "RestResponseScheduler";
    public static final int REST_RESPONSE_THREADS = 16;
    public static final int REST_RESPONSE_QUEUED_TASKS = 32;


    @Override
    protected void configure()
    {
        bind(String.class).annotatedWith(Names.named(CoreModule.MODULE_NAME))
            .toInstance(LinStor.CONTROLLER_MODULE);

        bind(FreeSpaceMgrMap.class).to(FreeSpaceMgrMapImpl.class);
        bind(AuthTokenMap.class).to(AuthTokenMapImpl.class);
    }

    @Provides
    @Singleton
    @Named(REST_RESPONSE_SCHEDULER)
    public Scheduler restResponseScheduler()
    {
        return Schedulers.newBoundedElastic(REST_RESPONSE_THREADS, REST_RESPONSE_QUEUED_TASKS, "RestResponse");
    }

    @Provides
    @Singleton
    @Named(LinStor.CONTROLLER_PROPS)
    public Props createControllerPropsContainer(PropsContainerFactory propsContainerFactory)
    {
        return propsContainerFactory.create(
            LinStorObject.CTRL.path,
            LinStorObject.CTRL.toString(),
            LinStorObject.CTRL
        );
    }

    @Provides
    @Singleton
    @Named(LinStor.CONTROLLER_PROPS)
    public ReadOnlyProps castCtrlPropsToReadOnlyProps(@Named(LinStor.CONTROLLER_PROPS) Props ctrlProps)
    {
        return ctrlProps;
    }

    public interface FreeSpaceMgrMap extends Map<SharedStorPoolName, FreeSpaceMgr>
    {
    }

    @Singleton
    public static class FreeSpaceMgrMapImpl
        extends TransactionMap<Void, SharedStorPoolName, FreeSpaceMgr> implements FreeSpaceMgrMap
    {
        @Inject
        public FreeSpaceMgrMapImpl(Provider<TransactionMgr> transMgrProvider)
        {
            super(null, new TreeMap<>(), null, transMgrProvider);
        }
    }

    public interface AuthTokenMap extends Map<Integer, AuthToken>
    {
    }

    @Singleton
    public static class AuthTokenMapImpl
        extends TransactionMap<Void, Integer, AuthToken> implements AuthTokenMap
    {
        @Inject
        public AuthTokenMapImpl(Provider<TransactionMgr> transMgrProvider)
        {
            super(null, new TreeMap<>(), null, transMgrProvider);
        }
    }
}
