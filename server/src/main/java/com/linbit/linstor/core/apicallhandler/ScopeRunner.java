package com.linbit.linstor.core.apicallhandler;

import com.linbit.linstor.annotation.ErrorReporterContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionMgrGenerator;
import com.linbit.linstor.transaction.TransactionMgrUtil;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.concurrent.Callable;
import java.util.function.Function;

import com.google.inject.Key;
import com.google.inject.name.Names;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

@Singleton
public class ScopeRunner
{
    private final ErrorReporter errorLog;
    private final TransactionMgrGenerator transactionMgrGenerator;
    private final LinStorScope apiCallScope;

    @Inject
    public ScopeRunner(
        ErrorReporter errorLogRef,
        TransactionMgrGenerator transactionMgrGeneratorRef,
        LinStorScope apiCallScopeRef
    )
    {
        errorLog = errorLogRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
        apiCallScope = apiCallScopeRef;
    }

    public <T> Flux<T> fluxInTransactionalScope(
        String scopeDescription,
        LockGuard lockGuard,
        Callable<Flux<T>> callable
    )
    {
        return fluxInScope(scopeDescription, lockGuard, callable, true);
    }

    public <T> Flux<T> fluxInTransactionlessScope(
        String scopeDescription,
        LockGuard lockGuard,
        Callable<Flux<T>> callable
    )
    {
        return fluxInScope(scopeDescription, lockGuard, callable, false);
    }

    public <T> Flux<T> fluxInScope(
        String scopeDescription,
        LockGuard lockGuard,
        Callable<Flux<T>> callable,
        boolean transactional
    )
    {
        return Mono.subscriberContext()
            .flatMapMany(subscriberContext -> Mono
                .fromCallable(() -> doInScope(subscriberContext, scopeDescription, lockGuard, callable, transactional))
                .flatMapMany(Function.identity())
            )
            .checkpoint(scopeDescription);
    }

    private <T> Flux<T> doInScope(
        Context subscriberContext,
        String scopeDescription,
        LockGuard lockGuard,
        Callable<Flux<T>> callable,
        boolean transactional
    )
        throws Exception
    {
        String apiCallName = subscriberContext.get(ApiModule.API_CALL_NAME);
        AccessContext accCtx = subscriberContext.get(AccessContext.class);
        Peer peer = subscriberContext.getOrDefault(Peer.class, null);
        Long apiCallId = subscriberContext.getOrDefault(ApiModule.API_CALL_ID, null);

        Flux<T> ret;

        String peerDescription;
        if (peer == null)
        {
            peerDescription = "";
        }
        else
        {
            peerDescription = "Peer " + peer + ", ";
        }

        String apiCallDescription;
        if (apiCallId == null)
        {
            apiCallDescription = "Background operation";
        }
        else if (apiCallId != 0L)
        {
            apiCallDescription = "API call " + apiCallId;
        }
        else
        {
            apiCallDescription = "oneway call";
        }

        errorLog.logTrace(
            "%s%s '%s' scope '%s' start", peerDescription, apiCallDescription, apiCallName, scopeDescription);

        TransactionMgr transMgr = transactional ? transactionMgrGenerator.startTransaction() : null;

        apiCallScope.enter();
        lockGuard.lock();
        try
        {
            apiCallScope.seed(Key.get(AccessContext.class, PeerContext.class), accCtx);
            apiCallScope.seed(Key.get(AccessContext.class, ErrorReporterContext.class), accCtx);
            if (peer != null)
            {
                apiCallScope.seed(Peer.class, peer);
            }
            if (apiCallId != null)
            {
                apiCallScope.seed(Key.get(Long.class, Names.named(ApiModule.API_CALL_ID)), apiCallId);
            }

            if (transMgr != null)
            {
                TransactionMgrUtil.seedTransactionMgr(apiCallScope, transMgr);
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
                    catch (TransactionException transExc)
                    {
                        errorLog.reportError(
                            Level.ERROR,
                            transExc,
                            accCtx,
                            peer,
                            "A database error occurred while trying to rollback '" + apiCallName + "'"
                        );
                    }
                }
                transMgr.returnConnection();
            }
            errorLog.logTrace(
                "%s%s '%s' scope '%s' end", peerDescription, apiCallDescription, apiCallName, scopeDescription);
        }

        return ret;
    }
}
