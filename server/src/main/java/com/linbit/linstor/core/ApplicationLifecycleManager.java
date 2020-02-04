package com.linbit.linstor.core;

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
import com.linbit.linstor.systemstarter.NetComServiceException;
import com.linbit.linstor.systemstarter.StartupInitializer;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;

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

    private boolean shutdownFinished;
    private WorkerPool workerThrPool;

    private ArrayList<StartupInitializer> services;

    @Inject
    public ApplicationLifecycleManager(
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        ShutdownProtHolder shutdownProtHolderRef,
        @Named(CoreModule.RECONFIGURATION_LOCK)
        ReadWriteLock reconfigurationLockRef
    )
    {
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        shutdownProtHolder = shutdownProtHolderRef;
        reconfigurationLock = reconfigurationLockRef;

        shutdownFinished = false;
    }

    public void installShutdownHook()
    {
        AccessContext shutdownCtx = sysCtx.clone();
        Runtime.getRuntime().addShutdownHook(new ModuleShutdownHook(shutdownCtx, this));
    }

    public void startSystemServices(ArrayList<StartupInitializer> services) throws NetComServiceException
    {
        this.services = services;
        // Start services
        for (StartupInitializer sysSvc : services)
        {
            if (sysSvc instanceof SystemService)
            {

                SystemService systemService = (SystemService) sysSvc;
                errorReporter.logInfo(
                String.format(
                    "Starting service instance '%s' of type %s",
                        systemService.getInstanceName().displayValue, systemService.getServiceName().displayValue
                )
            );
            }
            try
            {
                sysSvc.initialize();
            }
            catch (SystemServiceStartException startExc)
            {
                errorReporter.reportProblem(Level.ERROR, startExc, null, null, null);
            }
            catch (NetComServiceException exc)
            {
                throw exc;
            }
            catch (Exception unhandledExc)
            {
                errorReporter.reportError(unhandledExc);
            }
        }
    }

    public void stopSystemServices(ArrayList<StartupInitializer> services)
    {
        // Shutdown services in backwards order
        for (int i = services.size()-1; i >= 0; i--)
        {
            StartupInitializer service = services.get(i);
            if (service instanceof SystemService)
            {

                SystemService systemService = (SystemService) service;
                errorReporter.logInfo(
                    String.format(
                        "Shutting down service instance '%s' of type %s",
                        systemService.getInstanceName().displayValue, systemService.getServiceName().displayValue
                    )
                );
            }
            try
            {
                service.shutdown();

                if (service instanceof SystemService)
                {
                    errorReporter.logInfo(
                        String.format(
                            "Waiting for service instance '%s' to complete shutdown",
                            ((SystemService) service).getInstanceName().displayValue
                        )
                    );
                }
                service.awaitShutdown(SHUTDOWN_THR_JOIN_WAIT);
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
        shutdownProtHolder.getObjProt().requireAccess(accCtx, AccessType.USE);
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
            stopSystemServices(services);

            long exitTime = Math.addExact(System.currentTimeMillis(), SVC_SHUTDOWN_WAIT_TIME);
            for (StartupInitializer svc : services)
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
