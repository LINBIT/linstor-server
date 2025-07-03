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
import com.linbit.linstor.systemstarter.StartupInitializer;
import com.linbit.utils.CollectionUtils;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;

import org.slf4j.MDC;
import org.slf4j.event.Level;

@Singleton
public class ApplicationLifecycleManager
{
    // Maximum time to wait for services to shut down
    private static final long SVC_SHUTDOWN_WAIT_TIME = 10000L;

    // At shutdown, wait at most SHUTDOWN_THR_JOIN_WAIT milliseconds for
    // a service thread to end
    public static final long SHUTDOWN_THR_JOIN_WAIT = 3000L;

    private static final Object SHUTDOWN_SYNC_OBJ = new Object();

    private final AccessContext sysCtx;
    private final ErrorReporter errorReporter;
    private final ShutdownProtHolder shutdownProtHolder;
    private final ReadWriteLock reconfigurationLock;

    private @Nullable WorkerPool workerThrPool;

    private ArrayList<StartupInitializer> services;

    private boolean shutdownInitialized = false;

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
    }

    public void installShutdownHook()
    {
        AccessContext shutdownCtx = sysCtx.clone();
        Runtime.getRuntime().addShutdownHook(new ModuleShutdownHook(shutdownCtx, this));
    }

    public void startSystemServices(ArrayList<StartupInitializer> servicesRef)
        throws SystemServiceStartException
    {
        services = servicesRef;
        // Start services
        for (StartupInitializer service : services)
        {
            SystemService sysService = service.getSystemService();
            if (sysService != null)
            {
                errorReporter.logInfo(
                    String.format(
                        "Starting service instance '%s' of type %s",
                        sysService.getInstanceName().displayValue, sysService.getServiceName().displayValue
                )
            );
            }
            try
            {
                service.initialize();
            }
            catch (SystemServiceStartException startExc)
            {
                if (startExc.criticalError)
                {
                    throw startExc;
                }
                else
                {
                    errorReporter.reportError(startExc);
                }
            }
            catch (Exception unhandledExc)
            {
                errorReporter.reportError(unhandledExc);
                throw new SystemServiceStartException("Unhandled exception", unhandledExc, true);
            }
        }
    }

    private void shutdownSystemServices(ArrayList<StartupInitializer> serviceList)
    {
        // Shutdown services in backwards order
        for (int idx = serviceList.size() - 1; idx >= 0; idx--)
        {
            StartupInitializer service = serviceList.get(idx);
            SystemService sysService = service.getSystemService();
            if (sysService != null)
            {
                errorReporter.logInfo(
                    String.format(
                        "Shutting down service instance '%s' of type %s",
                        sysService.getInstanceName().displayValue, sysService.getServiceName().displayValue
                    )
                );
            }
            try
            {
                service.shutdown();
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
        errorReporter.logInfo("Shutdown in progress");

        ArrayList<StartupInitializer> localServices;
        localServices = initializeShutdown();
        waitForShutdownServices(localServices);

        shutdownWorkerThread();

        errorReporter.logInfo("Shutdown complete");
    }

    /**
     * Initialized the shutdown of all registered Services.
     *
     * @return A list of services to wait for their shutdown. List will be empty if the shutdown was already
     *         initialized by a different thread. In other words: Only the first thread will receive a non-empty list.
     */
    private ArrayList<StartupInitializer> initializeShutdown()
    {
        ArrayList<StartupInitializer> ret;
        synchronized (SHUTDOWN_SYNC_OBJ)
        {
            if (!shutdownInitialized)
            {
                try
                {
                    reconfigurationLock.writeLock().lock();
                    ret = new ArrayList<>(CollectionUtils.nonNullOrEmptyList(services));

                    // Shutdown service threads
                    shutdownSystemServices(ret); // does NOT wait until the services are fully shut down!
                }
                finally
                {
                    reconfigurationLock.writeLock().unlock();
                }
                shutdownInitialized = true;
            }
            else
            {
                ret = new ArrayList<>();
            }
        }
        return ret;
    }

    private void waitForShutdownServices(ArrayList<StartupInitializer> localServices)
    {
        try
        {
            long exitTime = Math.addExact(System.currentTimeMillis(), SVC_SHUTDOWN_WAIT_TIME);

            // Shutdown services in backwards order
            for (int idx = localServices.size() - 1; idx >= 0; idx--)
            {
                long now = System.currentTimeMillis();
                if (now < exitTime)
                {
                    StartupInitializer service = localServices.get(idx);
                    SystemService sysService = service.getSystemService();

                    if (sysService != null)
                    {
                        errorReporter.logInfo(
                            String.format(
                                "Waiting for service instance '%s' to complete shutdown",
                                sysService.getInstanceName().displayValue
                            )
                        );
                    }
                    try
                    {
                        service.awaitShutdown(Math.min(exitTime - now, SVC_SHUTDOWN_WAIT_TIME));
                    }
                    catch (InterruptedException ignored)
                    {
                    }
                    catch (Exception unhandledExc)
                    {
                        errorReporter.reportError(unhandledExc);
                    }
                }
            }
        }
        catch (Throwable error)
        {
            errorReporter.reportError(Level.ERROR, error);
        }
    }

    private void shutdownWorkerThread()
    {
        synchronized (SHUTDOWN_SYNC_OBJ)
        {
            try
            {
                // TODO: May want to add functionality so that all worker threads end
                // cleanly before proceeding with the shutdown procedure.
                // This must be done outside of the reconfigurationLock, because
                // worker threads may be waiting for that lock.
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
            try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
            {
                shutdownCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_MAC_OVRD, Privilege.PRIV_OBJ_USE);
            }
            catch (AccessDeniedException ignored)
            {
            }

            try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
            {
                lfCycMgr.executeShutdown();
            }
        }
    }
}
