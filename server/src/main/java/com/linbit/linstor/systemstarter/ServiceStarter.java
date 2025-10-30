package com.linbit.linstor.systemstarter;

import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;

public class ServiceStarter implements StartupInitializer
{
    private SystemService service;

    public ServiceStarter(SystemService serviceRef)
    {
        service = serviceRef;
    }

    @Override
    public void initialize()
        throws SystemServiceStartException
    {
        service.start();
    }

    @Override
    public void shutdown(boolean jvmShutdownRef)
    {
        service.shutdown(jvmShutdownRef);
    }

    @Override
    public void awaitShutdown(long timeout)
        throws InterruptedException
    {
        service.awaitShutdown(timeout);
    }

    @Override
    public SystemService getSystemService()
    {
        return service;
    }
}
