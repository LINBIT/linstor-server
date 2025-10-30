package com.linbit;

/**
 * Control interface for system services
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface SystemService extends SystemServiceInfo
{
    /**
     * Changes the service instance's name
     */
    void setServiceInstanceName(ServiceName instanceName);

    /**
     * Enables the service
     */
    void start() throws SystemServiceStartException;

    /**
     * Disables the service
     *
     * @param jvmShutdownRef <code>True</code> iff this shutdown was initiated by the shutdown hook or another
     *      source that should end in the end of the JVM relatively soon.
     */
    void shutdown(boolean jvmShutdownRef);

    /**
     * Waits until the service has shut down or the timeout is exceeded
     *
     * @param timeout Timeout for the operation in milliseconds
     * @throws java.lang.InterruptedException If the timeout is exceeded
     */
    void awaitShutdown(long timeout) throws InterruptedException;
}
