package com.linbit.linstor.core;

import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.WorkerPool;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.ShutdownProtHolder;

import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.slf4j.event.Level;

@Singleton
public class ApplicationLifecycleManager
{
    // Maximum time to wait for services to shut down
    private static final long SVC_SHUTDOWN_WAIT_TIME = 10000L;

    // At shutdown, wait at most SHUTDOWN_THR_JOIN_WAIT milliseconds for
    // a service thread to end
    public static final long SHUTDOWN_THR_JOIN_WAIT = 3000L;

    private final AccessContext sysCtx;
    private final ErrorReporter errorReporter;
    private final ShutdownProtHolder shutdownProtHolder;
    private final ReadWriteLock reconfigurationLock;
    private final Map<ServiceName, SystemService> systemServicesMap;

    private boolean shutdownFinished;
    private WorkerPool workerThrPool;

    @Inject
    public ApplicationLifecycleManager(
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        ShutdownProtHolder shutdownProtHolderRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        Map<ServiceName, SystemService> systemServicesMapRef
    )
    {
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        shutdownProtHolder = shutdownProtHolderRef;
        reconfigurationLock = reconfigurationLockRef;
        systemServicesMap = systemServicesMapRef;

        shutdownFinished = false;
    }

    public void installShutdownHook()
    {
        AccessContext shutdownCtx = sysCtx.clone();
        Runtime.getRuntime().addShutdownHook(new ModuleShutdownHook(shutdownCtx, this));
    }

    public void startSystemServices(Iterable<SystemService> services)
    {
        // Start services
        for (SystemService sysSvc : services)
        {
            errorReporter.logInfo(
                String.format(
                    "Starting service instance '%s' of type %s",
                    sysSvc.getInstanceName().displayValue, sysSvc.getServiceName().displayValue
                )
            );
            try
            {
                sysSvc.start();
            }
            catch (SystemServiceStartException startExc)
            {
                errorReporter.reportProblem(Level.ERROR, startExc, null, null, null);
            }
            catch (Exception unhandledExc)
            {
                errorReporter.reportError(unhandledExc);
            }
        }
    }

    public void stopSystemServices(Iterable<SystemService> services)
    {
        // Shutdown services
        for (SystemService sysSvc : services)
        {
            errorReporter.logInfo(
                String.format(
                    "Shutting down service instance '%s' of type %s",
                    sysSvc.getInstanceName().displayValue, sysSvc.getServiceName().displayValue
                )
            );
            try
            {
                sysSvc.shutdown();

                errorReporter.logInfo(
                    String.format(
                        "Waiting for service instance '%s' to complete shutdown",
                        sysSvc.getInstanceName().displayValue
                    )
                );
                sysSvc.awaitShutdown(SHUTDOWN_THR_JOIN_WAIT);
            }
            catch (Exception unhandledExc)
            {
                errorReporter.reportError(unhandledExc);
            }
        }
    }

    /**
     * Checks if the AccessContext has shutdown permissions.
     * Throws AccessDeniedException if the accCtx doesn't have access. otherwise runs as noop.
     *
     * @param accCtx AccessContext to check.
     * @throws AccessDeniedException if accCtx doesn't have shutdown access.
     */
    public void requireShutdownAccess(AccessContext accCtx) throws AccessDeniedException
    {
        shutdownProtHolder.getShutdownProt().requireAccess(accCtx, AccessType.USE);
    }

    public void shutdown(AccessContext accCtx) throws AccessDeniedException
    {
        requireShutdownAccess(accCtx);

        errorReporter.logInfo(
            String.format(
                "Shutdown request by subject '%s' using role '%s'\n",
                accCtx.getIdentity(), accCtx.getRole()
            )
        );

        // The executeShutdown() method is invoked by ModuleShutdownHook
        System.exit(InternalApiConsts.EXIT_CODE_SHUTDOWN);
    }

    private void executeShutdown()
    {
        try
        {
            errorReporter.logInfo("Shutdown in progress");

            // TODO: May want to add functionality so that all worker threads end
            //       cleanly before proceeding with the shutdown procedure.
            //       This must be done outside of the reconfigurationLock, because
            //       worker threads may be waiting for that lock.
            if (workerThrPool != null)
            {
                errorReporter.logInfo("Shutting down worker thread pool");
                workerThrPool.shutdown();
                workerThrPool = null;
            }
        }
        catch (Throwable error)
        {
            errorReporter.reportError(Level.ERROR, error);
        }

        try
        {
            reconfigurationLock.writeLock().lock();

            // Shutdown service threads
            stopSystemServices(systemServicesMap.values());

            long exitTime = Math.addExact(System.currentTimeMillis(), SVC_SHUTDOWN_WAIT_TIME);
            for (SystemService svc  : systemServicesMap.values())
            {
                long now = System.currentTimeMillis();
                if (now < exitTime)
                {
                    long maxWaitTime = exitTime - now;
                    if (maxWaitTime > SVC_SHUTDOWN_WAIT_TIME)
                    {
                        maxWaitTime = SVC_SHUTDOWN_WAIT_TIME;
                    }

                    try
                    {
                        svc.awaitShutdown(maxWaitTime);
                    }
                    catch (InterruptedException ignored)
                    {
                    }
                    catch (Throwable error)
                    {
                        errorReporter.reportError(Level.ERROR, error);
                    }
                }
                else
                {
                    break;
                }
            }

            errorReporter.logInfo("Shutdown complete");
        }
        catch (Throwable error)
        {
            errorReporter.reportError(Level.ERROR, error);
        }
        finally
        {
            reconfigurationLock.writeLock().unlock();
        }
    }

    static class ModuleShutdownHook extends Thread
    {
        private AccessContext shutdownCtx;
        private ApplicationLifecycleManager lfCycMgr;

        private ModuleShutdownHook(AccessContext privCtx, ApplicationLifecycleManager lfCycMgrRef)
        {
            shutdownCtx = privCtx;
            lfCycMgr = lfCycMgrRef;
        }

        @Override
        public void run()
        {
            try
            {
                shutdownCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_MAC_OVRD, Privilege.PRIV_OBJ_USE);
            }
            catch (AccessDeniedException ignored)
            {
            }

            lfCycMgr.executeShutdown();
        }
    }
}
