package com.linbit.linstor.api.rest.v1.config;

import com.google.inject.Injector;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.internal.inject.InjectionManager;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.server.spi.ContainerLifecycleListener;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;

public class GuiceResourceConfig extends ResourceConfig
{
    public GuiceResourceConfig(Injector injector)
    {
        register(new ContainerLifecycleListener()
        {
            public void onStartup(Container container)
            {
                InjectionManager im = container.getApplicationHandler().getInjectionManager();
                ServiceLocator serviceLocator = im.getInstance(ServiceLocator.class);
                GuiceBridge.getGuiceBridge().initializeGuiceBridge(serviceLocator);
                GuiceIntoHK2Bridge guiceBridge = serviceLocator.getService(GuiceIntoHK2Bridge.class);
                guiceBridge.bridgeGuiceInjector(injector);
            }
            public void onReload(Container container)
            {
            }
            public void onShutdown(Container container)
            {
            }
        });
    }
}
