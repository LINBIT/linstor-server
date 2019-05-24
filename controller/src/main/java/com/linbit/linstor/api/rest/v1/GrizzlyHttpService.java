package com.linbit.linstor.api.rest.v1;

import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.logging.ErrorReporter;

import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import com.google.inject.Injector;
import org.glassfish.grizzly.http.CompressionConfig;
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
    private final ErrorReporter errorReporter;
    private HttpServer httpServer;
    private ServiceName instanceName;
    private String listenAddress;
    private ResourceConfig v1ResourceConfig;

    private static final String INDEX_CONTENT = "<html><title>Linstor REST server</title>" +
        "<body><a href=\"https://app.swaggerhub.com/apis-docs/Linstor/Linstor/" + JsonGenTypes.REST_API_VERSION
        + "\">Documentation</a></body></html>";

    private static final int COMPRESSION_MIN_SIZE = 1000; // didn't find a good default, so lets say 1000

    public GrizzlyHttpService(Injector injector, ErrorReporter errorReporterRef, String listenAddressRef)
    {
        errorReporter = errorReporterRef;
        listenAddress = listenAddressRef;
        v1ResourceConfig = new GuiceResourceConfig(injector).packages("com.linbit.linstor.api.rest.v1");
        v1ResourceConfig.register(new CORSFilter());

        initGrizzly(listenAddress);

        try
        {
            instanceName = new ServiceName("GrizzlyHttpServer");
        }
        catch (InvalidNameException ignored)
        {
        }
    }

    private void initGrizzly(final String bindAddress)
    {
        httpServer = GrizzlyHttpServerFactory.createHttpServer(
            URI.create(String.format("http://%s/v1/", bindAddress)),
            v1ResourceConfig,
            false
        );

        CompressionConfig compressionConfig = httpServer.getListener("grizzly").getCompressionConfig();
        compressionConfig.setCompressionMode(CompressionConfig.CompressionMode.ON);
        compressionConfig.setCompressibleMimeTypes("text/plain", "text/html", "application/json");
        compressionConfig.setCompressionMinSize(COMPRESSION_MIN_SIZE);


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

        final AccessLogBuilder builder = new AccessLogBuilder(
            errorReporter.getLogDirectory().resolve("rest-access.log").toFile()
        );
        builder.instrument(httpServer.getServerConfiguration());
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
        catch (SocketException sexc)
        {
            errorReporter.logError("Unable to start grizzly http server on " + listenAddress + ".");
            // ipv6 failed, if it is localhost ipv6, retry ipv4
            if (listenAddress.startsWith("[::]"))
            {
                URI uri = URI.create(String.format("http://%s/v1/", listenAddress));
                errorReporter.logInfo("Trying to start grizzly http server on fallback ipv4: 0.0.0.0:" + uri.getPort());
                try
                {
                    initGrizzly("0.0.0.0:" + uri.getPort());
                    httpServer.start();
                }
                catch (IOException exc)
                {
                    throw new SystemServiceStartException("Unable to start grizzly http server on fallback ipv4", exc);
                }
            }
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
