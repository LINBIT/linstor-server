package com.linbit.linstor.core.apicallhandler;

import com.google.inject.Key;
import com.google.inject.name.Names;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.locks.LockGuard;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.function.Function;

@Singleton
public class ScopeRunner
{
    private final ErrorReporter errorLog;
    private final Provider<TransactionMgr> trnActProvider;
    private final LinStorScope apiCallScope;

    @Inject
    public ScopeRunner(
        ErrorReporter errorLogRef,
        @Named(LinStorModule.TRANS_MGR_GENERATOR) Provider<TransactionMgr> trnActProviderRef,
        LinStorScope apiCallScopeRef
    )
    {
        errorLog = errorLogRef;
        trnActProvider = trnActProviderRef;
        apiCallScope = apiCallScopeRef;
    }

    public <T> Flux<T> fluxInTransactionalScope(LockGuard lockGuard, Callable<Flux<T>> callable)
    {
        return fluxInScope(lockGuard, callable, true);
    }

    public <T> Flux<T> fluxInTransactionlessScope(LockGuard lockGuard, Callable<Flux<T>> callable)
    {
        return fluxInScope(lockGuard, callable, false);
    }

    private <T> Flux<T> fluxInScope(
        LockGuard lockGuard,
        Callable<Flux<T>> callable,
        boolean transactional
    )
    {
        return Mono.subscriberContext()
            .flatMapMany(subscriberContext ->
                Mono.fromCallable(() -> doInScope(subscriberContext, callable, lockGuard, transactional))
                    .flatMapMany(Function.identity())
            );
    }

    private <T> Flux<T> doInScope(
        Context subscriberContext,
        Callable<Flux<T>> callable,
        LockGuard lockGuard,
        boolean transactional
    )
        throws Exception
    {
        String apiCallName = subscriberContext.get(ApiModule.API_CALL_NAME);
        Peer peer = subscriberContext.get(Peer.class);
        Long apiCallId = subscriberContext.get(ApiModule.API_CALL_ID);

        Flux<T> ret;

        errorLog.logTrace("Running in scope of ApiCall '%s' from %s start", apiCallName, peer);

        TransactionMgr transMgr = transactional ? trnActProvider.get() : null;

        apiCallScope.enter();
        lockGuard.lock();
        try
        {
            apiCallScope.seed(Key.get(AccessContext.class, PeerContext.class), peer.getAccessContext());
            apiCallScope.seed(Peer.class, peer);
            apiCallScope.seed(Key.get(Long.class, Names.named(ApiModule.API_CALL_ID)), apiCallId);

            if (transMgr != null)
            {
                apiCallScope.seed(TransactionMgr.class, transMgr);
            }

            ret = callable.call();
        }
        finally
        {
            lockGuard.unlock();
            apiCallScope.exit();
            if (transMgr != null)
            {
                if (transMgr.isDirty())
                {
                    try
                    {
                        transMgr.rollback();
                    }
                    catch (SQLException sqlExc)
                    {
                        errorLog.reportError(
                            Level.ERROR,
                            sqlExc,
                            peer.getAccessContext(),
                            peer,
                            "A database error occured while trying to rollback '" + apiCallName + "'"
                        );
                    }
                }
                transMgr.returnConnection();
            }
            errorLog.logTrace("Running in scope of ApiCall '%s' from %s finished", apiCallName, peer);
        }

        return ret;
    }
}
