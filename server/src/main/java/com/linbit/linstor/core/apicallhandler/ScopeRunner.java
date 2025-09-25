package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ErrorReporterContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

import com.google.inject.Key;
import com.google.inject.name.Names;
import org.slf4j.MDC;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

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
        return fluxInScope(scopeDescription, lockGuard, callable, true, MDC.getCopyOfContextMap());
    }

    public <T> Flux<T> fluxInTransactionalScope(
        String scopeDescription,
        LockGuard lockGuard,
        Callable<Flux<T>> callable,
        Map<String, String> logContextMap
    )
    {
        return fluxInScope(scopeDescription, lockGuard, callable, true, logContextMap);
    }

    public <T> Flux<T> fluxInTransactionlessScope(
        String scopeDescription,
        LockGuard lockGuard,
        Callable<Flux<T>> callable
    )
    {
        return fluxInScope(scopeDescription, lockGuard, callable, false, MDC.getCopyOfContextMap());
    }

    public <T> Flux<T> fluxInTransactionlessScope(
        String scopeDescription,
        LockGuard lockGuard,
        Callable<Flux<T>> callable,
        Map<String, String> logContextMap
    )
    {
        return fluxInScope(scopeDescription, lockGuard, callable, false, logContextMap);
    }

    public <T> Flux<T> fluxInScope(
        String scopeDescription,
        LockGuard lockGuard,
        Callable<Flux<T>> callable,
        boolean transactional,
        Map<String, String> logContextMap
    )
    {
        return Mono.deferContextual(Mono::just)
            .flatMapMany(subscriberContext -> Mono
                .fromCallable(() ->
                    doInScope(subscriberContext, scopeDescription, lockGuard, callable, transactional, logContextMap))
                .flatMapMany(Function.identity())
            )
            .checkpoint(scopeDescription);
    }

    private <T> Flux<T> doInScope(
        ContextView subscriberContext,
        String scopeDescription,
        LockGuard lockGuard,
        Callable<Flux<T>> callable,
        boolean transactional,
        Map<String, String> logContextMap
    )
        throws Exception
    {
        MDC.setContextMap(logContextMap);
        String apiCallName = subscriberContext.get(ApiModule.API_CALL_NAME);
        AccessContext accCtx = subscriberContext.get(AccessContext.class);
        @Nullable Peer peer = subscriberContext.getOrDefault(Peer.class, null);
        @Nullable Long apiCallId = subscriberContext.getOrDefault(ApiModule.API_CALL_ID, null);

        @Nullable Flux<T> ret;

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
        else
        if (apiCallId != 0L)
        {
            apiCallDescription = "API call " + apiCallId;
        }
        else
        {
            apiCallDescription = "oneway call";
        }

        errorLog.logTrace(
            "%s%s '%s' scope '%s' start", peerDescription, apiCallDescription, apiCallName, scopeDescription);

        @Nullable Exception caughtExc = null;
        @Nullable ImplementationError caughtImplError = null;
        // start transaction before locks are acquired, just as all other calls do. Some calls simply cannot reverse
        // the order. The database issues should be able to be avoided using the setAutoCommit described in
        // f068f9b839a6f18f08d5e84a7fdaa71a4fd13d6f
        @Nullable TransactionMgr transMgr = transactional ? transactionMgrGenerator.startTransaction() : null;
        lockGuard.lock();
        try (LinStorScope.ScopeAutoCloseable close = apiCallScope.enter())
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
        catch (Exception exc)
        {
            ret = null; // suppress not initialized error
            caughtExc = exc;
        }
        catch (ImplementationError implErr)
        {
            ret = null; // suppress not initialized error
            caughtImplError = implErr;
        }
        finally
        {
            try
            {
                rollbackIfNeeded(apiCallName, accCtx, peer, transMgr);
                errorLog.logTrace(
                    "%s%s '%s' scope '%s' end",
                    peerDescription,
                    apiCallDescription,
                    apiCallName,
                    scopeDescription
                );
            }
            catch (Exception | ImplementationError exc)
            {
                if (caughtExc != null)
                {
                    exc.addSuppressed(caughtExc);
                }
                throw exc;
            }
            finally
            {
                lockGuard.unlock();
            }

            if (caughtExc != null)
            {
                throw caughtExc;
            }
            if (caughtImplError != null)
            {
                throw caughtImplError;
            }
            MDC.remove(ErrorReporter.LOGID);
        }

        return ret;
    }

    private void rollbackIfNeeded(
        String apiCallName,
        AccessContext accCtx,
        @Nullable Peer peer,
        @Nullable TransactionMgr transMgr
    )
    {
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
                    // TODO: needs a better panic-shutdown to prevent DB corruption
                    System.exit(1);
                }
            }
            transMgr.returnConnection();
        }
    }
}
