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
    void setServiceInstanceName(String instanceName);

    /**
     * Enables the service
     */
    void start();

    /**
     * Disables the service
     */
    void shutdown();
}
