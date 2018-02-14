package com.linbit.linstor.core;

import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.WorkerPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.SecurityModule;
import com.linbit.utils.MathUtils;
import org.slf4j.event.Level;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

@Singleton
public class ApplicationLifecycleManager
{
    // Maximum time to wait for services to shut down
    private static final long SVC_SHUTDOWN_WAIT_TIME = 10000L;

    // At shutdown, wait at most SHUTDOWN_THR_JOIN_WAIT milliseconds for
    // a service thread to end
    public static final long SHUTDOWN_THR_JOIN_WAIT = 3000L;

    private final ErrorReporter errorReporter;
    private final ObjectProtection shutdownProt;
    private final ReadWriteLock reconfigurationLock;
    private final Map<ServiceName, SystemService> systemServicesMap;

    private boolean shutdownFinished;
    private WorkerPool workerThrPool;

    @Inject
    public ApplicationLifecycleManager(
        ErrorReporter errorReporterRef,
        @Named(SecurityModule.SHUTDOWN_PROT) ObjectProtection shutdownProtRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        Map<ServiceName, SystemService> systemServicesMapRef
    )
    {
        errorReporter = errorReporterRef;
        shutdownProt = shutdownProtRef;
        reconfigurationLock = reconfigurationLockRef;
        systemServicesMap = systemServicesMapRef;

        shutdownFinished = false;
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
        shutdownProt.requireAccess(accCtx, AccessType.USE);
    }

    public void shutdown(AccessContext accCtx) throws AccessDeniedException
    {
        shutdown(accCtx, true);
    }

    public void shutdown(AccessContext accCtx, boolean sysExit) throws AccessDeniedException
    {
        requireShutdownAccess(accCtx);

        try
        {
            reconfigurationLock.writeLock().lock();
            if (!shutdownFinished)
            {
                errorReporter.logInfo(
                    String.format(
                        "Shutdown initiated by subject '%s' using role '%s'\n",
                        accCtx.getIdentity(), accCtx.getRole()
                    )
                );

                errorReporter.logInfo("Shutdown in progress");

                // Shutdown service threads
                stopSystemServices(systemServicesMap.values());

                if (workerThrPool != null)
                {
                    errorReporter.logInfo("Shutting down worker thread pool");
                    workerThrPool.shutdown();
                    workerThrPool = null;
                }

                long exitTime = MathUtils.addExact(System.currentTimeMillis(), SVC_SHUTDOWN_WAIT_TIME);
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
            shutdownFinished = true;
        }
        catch (Throwable error)
        {
            errorReporter.reportError(Level.ERROR, error);
        }
        finally
        {
            reconfigurationLock.writeLock().unlock();
        }
        if (sysExit)
        {
            System.exit(0);
        }
    }

}
