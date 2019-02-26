package com.linbit.linstor.api.rest.v1;

import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import com.google.inject.Injector;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.accesslog.AccessLogBuilder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

public class GrizzlyHttpService implements SystemService
{
    private final HttpServer httpServer;
    private ServiceName instanceName;

    public GrizzlyHttpService(Injector injector, Path logDirectory, String listenAddress, int port)
    {
        ResourceConfig resourceConfig = new GuiceResourceConfig(injector).packages("com.linbit.linstor.api.rest.v1");
        resourceConfig.register(new CORSFilter());

        httpServer = GrizzlyHttpServerFactory.createHttpServer(
            URI.create(String.format("http://%s:%d/v1/", listenAddress, port)),
            resourceConfig,
            false
        );

        final AccessLogBuilder builder = new AccessLogBuilder(logDirectory.resolve("rest-access.log").toFile());
        builder.instrument(httpServer.getServerConfiguration());

        try
        {
            instanceName = new ServiceName("GrizzlyHttpServer");
        }
        catch (InvalidNameException ignored)
        {
        }
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceNameRef)
    {
        instanceName = instanceNameRef;
    }

    @Override
    public void start() throws SystemServiceStartException
    {
        try
        {
            httpServer.start();
        }
        catch (IOException exc)
        {
            throw new SystemServiceStartException("Unable to start grizzly http server", exc);
        }
    }

    @Override
    public void shutdown()
    {
        httpServer.shutdownNow();
    }

    @Override
    public void awaitShutdown(long timeout)
    {
        httpServer.shutdown(timeout, TimeUnit.SECONDS);
    }

    @Override
    public ServiceName getServiceName()
    {
        ServiceName svcName = null;
        try
        {
            svcName = new ServiceName("Grizzly-HTTP-Server");
        }
        catch (InvalidNameException ignored)
        {
        }
        return svcName;
    }

    @Override
    public String getServiceInfo()
    {
        return "Grizzly HTTP server";
    }

    @Override
    public ServiceName getInstanceName()
    {
        return instanceName;
    }

    @Override
    public boolean isStarted()
    {
        return httpServer.isStarted();
    }
}
