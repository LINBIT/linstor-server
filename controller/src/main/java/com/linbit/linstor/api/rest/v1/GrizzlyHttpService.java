package com.linbit.linstor.api.rest.v1;

import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.TBL_SEC_CONFIGURATION;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ExceptionMapper;
import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.inject.Injector;
import org.glassfish.grizzly.http.CompressionConfig;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.server.accesslog.AccessLogBuilder;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.glassfish.grizzly.ssl.SSLContextConfigurator;
import org.glassfish.grizzly.ssl.SSLEngineConfigurator;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

public class GrizzlyHttpService implements SystemService
{
    private final ErrorReporter errorReporter;
    private HttpServer httpServer;
    private HttpServer httpsServer;
    private ServiceName instanceName;
    private String listenAddress;
    private String listenAddressSecure;
    private Path keyStoreFile;
    private String keyStorePassword;
    private ResourceConfig v1ResourceConfig;
    private final DbConnectionPool dbConnectionPool;
    private final Map<ServiceName, SystemService> systemServiceMap;

    private static final String INDEX_CONTENT = "<html><title>Linstor REST server</title>" +
        "<body><a href=\"https://app.swaggerhub.com/apis-docs/Linstor/Linstor/" + JsonGenTypes.REST_API_VERSION +
        "\">Documentation</a></body></html>";

    private static final int COMPRESSION_MIN_SIZE = 1000; // didn't find a good default, so lets say 1000

    public GrizzlyHttpService(
        Injector injector,
        ErrorReporter errorReporterRef,
        Map<ServiceName, SystemService> systemServiceMapRef,
        String listenAddressRef,
        String listenAddressSecureRef,
        Path keyStoreFileRef,
        String keyStorePasswordRef
    )
    {
        errorReporter = errorReporterRef;
        dbConnectionPool = injector.getInstance(DbConnectionPool.class);
        listenAddress = listenAddressRef;
        listenAddressSecure = listenAddressSecureRef;
        keyStoreFile = keyStoreFileRef;
        keyStorePassword = keyStorePasswordRef;
        v1ResourceConfig = new GuiceResourceConfig(injector).packages("com.linbit.linstor.api.rest.v1");
        v1ResourceConfig.register(new CORSFilter());
        registerExceptionMappers(v1ResourceConfig);
        systemServiceMap = systemServiceMapRef;

        try
        {
            instanceName = new ServiceName("GrizzlyHttpServer");
        }
        catch (InvalidNameException ignored)
        {
        }
    }

    private void addRootHandler(HttpServer httpServerRef)
    {
        httpServerRef.getServerConfiguration().addHttpHandler(
            new HttpHandler()
            {
                @Override
                public void service(Request request, Response response) throws Exception
                {
                    if (request.getMethod() == Method.GET)
                    {
                        if (request.getHttpHandlerPath().equals("/"))
                        {
                            response.setContentType("text/html");
                            response.setContentLength(INDEX_CONTENT.length());
                            response.getWriter().write(INDEX_CONTENT);
                        }
                        else if (request.getHttpHandlerPath().equals("/health"))
                        {
                            Connection conn = null;
                            try
                            {
                                conn = dbConnectionPool.getConnection();
                                conn.createStatement().executeQuery("SELECT 1 FROM " + TBL_SEC_CONFIGURATION);

                                List<String> notRunning = systemServiceMap.values().stream()
                                    .filter(service -> !service.isStarted())
                                    .map(service -> service.getServiceName().getDisplayName())
                                    .collect(Collectors.toList());
                                if (notRunning.isEmpty())
                                {
                                    response.setStatus(HttpStatus.OK_200);
                                }
                                else
                                {
                                    final String errorMsg = "Services not running: " +
                                        String.join(",", notRunning);
                                    response.setContentLength(errorMsg.length());
                                    response.getWriter().write(errorMsg);
                                    response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
                                }
                            }
                            catch (SQLException exc)
                            {
                                final String errorMsg = "Failed to connect to database: " + exc.getMessage();
                                response.setContentLength(errorMsg.length());
                                response.getWriter().write(errorMsg);
                                response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
                            }
                            finally
                            {
                                if (conn != null)
                                {
                                    dbConnectionPool.returnConnection(conn);
                                }
                            }
                        }
                        else
                        {
                            response.setStatus(HttpStatus.NOT_FOUND_404);
                        }
                    }
                    else
                    {
                        response.setStatus(HttpStatus.NOT_FOUND_404);
                    }
                }
            }
        );
    }

    private void addHTTPSRedirectHandler(HttpServer httpServerRef, int httpsPort)
    {
        httpServerRef.getServerConfiguration().addHttpHandler(
            new HttpHandler()
            {
                @Override
                public void service(Request request, Response response) throws Exception
                {
                    if (request.getMethod() == Method.GET)
                    {
                        if (request.getHttpHandlerPath().equals("/"))
                        {
                            response.setContentType("text/html");
                            response.setContentLength(INDEX_CONTENT.length());
                            response.getWriter().write(INDEX_CONTENT);
                        }
                        else if (request.getHttpHandlerPath().equals("/health"))
                        {
                            Connection conn = null;
                            try
                            {
                                conn = dbConnectionPool.getConnection();
                                conn.createStatement().executeQuery("SELECT 1 FROM " + TBL_SEC_CONFIGURATION);

                                List<String> notRunning = systemServiceMap.values().stream()
                                    .filter(service -> !service.isStarted())
                                    .map(service -> service.getServiceName().getDisplayName())
                                    .collect(Collectors.toList());
                                if (notRunning.isEmpty())
                                {
                                    response.setStatus(HttpStatus.OK_200);
                                }
                                else
                                {
                                    final String errorMsg = "Services not running: " +
                                        String.join(",", notRunning);
                                    response.setContentLength(errorMsg.length());
                                    response.getWriter().write(errorMsg);
                                    response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
                                }
                            }
                            catch (SQLException exc)
                            {
                                final String errorMsg = "Failed to connect to database: " + exc.getMessage();
                                response.setContentLength(errorMsg.length());
                                response.getWriter().write(errorMsg);
                                response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
                            }
                            finally
                            {
                                if (conn != null)
                                {
                                    dbConnectionPool.returnConnection(conn);
                                }
                            }
                        }
                        else
                        {
                            response.setStatus(HttpStatus.NOT_FOUND_404);
                            response.sendRedirect(
                                String.format("https://%s:%d", request.getServerName(), httpsPort) +
                                    request.getHttpHandlerPath()
                            );
                        }
                    }
                    else
                    {
                        response.setStatus(HttpStatus.NOT_FOUND_404);
                        response.sendRedirect(
                            String.format("https://%s:%d", request.getServerName(), httpsPort) +
                            request.getHttpHandlerPath());
                    }
                }
            }
        );
    }

    private void enableCompression(HttpServer httpServerRef)
    {
        CompressionConfig compressionConfig = httpServerRef.getListener("grizzly").getCompressionConfig();
        compressionConfig.setCompressionMode(CompressionConfig.CompressionMode.ON);
        compressionConfig.setCompressibleMimeTypes("text/plain", "text/html", "application/json");
        compressionConfig.setCompressionMinSize(COMPRESSION_MIN_SIZE);
    }

    private void initGrizzly(final String bindAddress, final String httpsBindAddress)
    {
        final AccessLogBuilder builder = new AccessLogBuilder(
            errorReporter.getLogDirectory().resolve("rest-access.log").toFile()
        );

        if (keyStoreFile != null)
        {
            final URI httpsUri = URI.create(String.format("https://%s/v1/", httpsBindAddress));

            // only install a redirect handler for http
            httpServer = GrizzlyHttpServerFactory.createHttpServer(
                URI.create(String.format("http://%s", bindAddress)),
                false
            );

            addHTTPSRedirectHandler(httpServer, httpsUri.getPort());

            httpsServer = GrizzlyHttpServerFactory.createHttpServer(
                httpsUri,
                v1ResourceConfig,
                false
            );

            SSLContextConfigurator sslCon = new SSLContextConfigurator();
            sslCon.setSecurityProtocol("TLS");
            sslCon.setKeyStoreFile(keyStoreFile.toString());
            sslCon.setKeyStorePass(keyStorePassword);

            for (NetworkListener netListener : httpsServer.getListeners())
            {
                netListener.setSecure(true);
                SSLEngineConfigurator ssle = new SSLEngineConfigurator(sslCon);
                ssle.setWantClientAuth(false);
                ssle.setClientMode(false);
                ssle.setNeedClientAuth(false);
                netListener.setSSLEngineConfig(ssle);
            }

            enableCompression(httpsServer);

            addRootHandler(httpsServer);

            builder.instrument(httpServer.getServerConfiguration());
            builder.instrument(httpsServer.getServerConfiguration());
        }
        else
        {
            httpsServer = null;
            httpServer = GrizzlyHttpServerFactory.createHttpServer(
                URI.create(String.format("http://%s/v1/", bindAddress)),
                v1ResourceConfig,
                false
            );

            addRootHandler(httpServer);

            builder.instrument(httpServer.getServerConfiguration());
        }

        enableCompression(httpServer);
    }

    private void registerExceptionMappers(ResourceConfig resourceConfig)
    {
        resourceConfig.register(new ExceptionMapper<Exception>()
        {
            @Override
            public javax.ws.rs.core.Response toResponse(Exception exc)
            {
                String errorReport = errorReporter.reportError(exc);
                javax.ws.rs.core.Response.Status respStatus;

                ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                if (exc instanceof ApiRcException)
                {
                    apiCallRc.addEntries(((ApiRcException) exc).getApiCallRc());
                    respStatus = javax.ws.rs.core.Response.Status.BAD_REQUEST;
                }
                else if (exc instanceof JsonMappingException ||
                    exc instanceof JsonParseException)
                {
                    apiCallRc.addEntry(
                        ApiCallRcImpl.entryBuilder(
                            ApiConsts.API_CALL_PARSE_ERROR,
                            "Unable to parse input json."
                        )
                        .setDetails(exc.getMessage())
                        .addErrorId(errorReport)
                        .build()
                    );
                    respStatus = javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
                }
                else
                {
                    apiCallRc.addEntry(
                        ApiCallRcImpl.entryBuilder(
                            ApiConsts.FAIL_UNKNOWN_ERROR,
                            "An unknown error occurred."
                        )
                        .setDetails(exc.getMessage())
                        .addErrorId(errorReport)
                        .build()
                    );
                    respStatus = javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
                }

                return javax.ws.rs.core.Response
                    .status(respStatus)
                    .type(MediaType.APPLICATION_JSON)
                    .entity(ApiCallRcConverter.toJSON(apiCallRc))
                    .build();
            }
        });
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
            initGrizzly(listenAddress, listenAddressSecure);
            httpServer.start();

            if (httpsServer != null)
            {
                httpsServer.start();
            }
        }
        catch (SocketException sexc)
        {
            errorReporter.logError("Unable to start grizzly http server on " + listenAddress + ".");
            // ipv6 failed, if it is localhost ipv6, retry ipv4
            if (listenAddress.startsWith("[::]"))
            {
                URI uri = URI.create(String.format("http://%s/v1/", listenAddress));
                URI uriSecure = URI.create(String.format("https://%s/v1/", listenAddressSecure));
                errorReporter.logInfo("Trying to start grizzly http server on fallback ipv4: 0.0.0.0");
                try
                {
                    initGrizzly("0.0.0.0:" + uri.getPort(), "0.0.0.0:" + uriSecure.getPort());
                    httpServer.start();

                    if (httpsServer != null)
                    {
                        httpsServer.start();
                    }
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
        if (httpsServer != null)
        {
            httpsServer.shutdownNow();
        }
    }

    @Override
    public void awaitShutdown(long timeout)
    {
        httpServer.shutdown(timeout, TimeUnit.SECONDS);
        if (httpsServer != null)
        {
            httpsServer.shutdown(timeout, TimeUnit.SECONDS);
        }
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
