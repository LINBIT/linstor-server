package com.linbit;

/**
 * Information interface for system services
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface SystemServiceInfo
{
    /**
     * Returns the name for all instances of this service
     */
    ServiceName getServiceName();

    /**
     * Returns a short description of what the service is,
     * e.g. "Worker thread pool service"
     */
    String getServiceInfo();

    /**
     * Returns the name of this instance of the service
     */
    ServiceName getInstanceName();

    /**
     * Indicates whether the service is started or not
     */
    boolean isStarted();
}
