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
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.server.accesslog.AccessLogBuilder;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

public class GrizzlyHttpService implements SystemService
{
    private final HttpServer httpServer;
    private ServiceName instanceName;

    private static final String INDEX_CONTENT = "<html><title>Linstor REST server</title>" +
        "<body><a href=\"https://app.swaggerhub.com/apis-docs/Linstor/Linstor/1.0.0\">Documentation</a>" +
        "</body></html>";

    public GrizzlyHttpService(Injector injector, Path logDirectory, String listenAddress)
    {
        ResourceConfig resourceConfig = new GuiceResourceConfig(injector).packages("com.linbit.linstor.api.rest.v1");
        resourceConfig.register(new CORSFilter());

        httpServer = GrizzlyHttpServerFactory.createHttpServer(
            URI.create(String.format("http://%s/v1/", listenAddress)),
            resourceConfig,
            false
        );

        httpServer.getServerConfiguration().addHttpHandler(
            new HttpHandler()
            {
                @Override
                public void service(Request request, Response response) throws Exception
                {
                    if (request.getMethod() == Method.GET && request.getHttpHandlerPath().equals("/"))
                    {
                        response.setContentType("text/html");
                        response.setContentLength(INDEX_CONTENT.length());
                        response.getWriter().write(INDEX_CONTENT);
                    }
                    else
                    {
                        response.setStatus(HttpStatus.NOT_FOUND_404);
                    }
                }
            }
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
