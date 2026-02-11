package com.linbit.linstor.api.rest.v1.config;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.cfg.LinstorConfig;
import com.linbit.linstor.core.cfg.LinstorConfig.RestAccessLogMode;
import com.linbit.linstor.core.repository.AuthTokenRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.locks.LockGuardFactory.LockObj.CTRL_CONFIG;
import static com.linbit.locks.LockGuardFactory.LockType.READ;

import javax.ws.rs.NotAllowedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.inject.Injector;
import com.google.inject.Key;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.glassfish.grizzly.http.CompressionConfig;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.server.StaticHttpHandler;
import org.glassfish.grizzly.http.server.accesslog.AccessLogBuilder;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.glassfish.grizzly.ssl.SSLContextConfigurator;
import org.glassfish.grizzly.ssl.SSLEngineConfigurator;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

public class GrizzlyHttpService implements SystemService
{
    private static final int COMPRESSION_MIN_SIZE = 1000; // didn't find a good default, so lets say 1000
    private static final Path AUTO_HTTPS_KEYSTORE_PATH = Paths.get("/var/lib/linstor/autohttps-keystore.p12");

    private final ErrorReporter errorReporter;
    private final String listenAddress;
    private final String listenAddressSecure;
    private final @Nullable Path keyStoreFile;
    private final @Nullable String keyStorePassword;
    private final @Nullable Path trustStoreFile;
    private final @Nullable String trustStorePassword;
    private final ResourceConfig restResourceConfig;
    private final Path restAccessLogPath;
    private final SystemConfRepository systemConfRepository;
    private final AccessContext sysCtx;
    private final LockGuardFactory lockGuardFactory;
    private final String webUiDirectory;

    private @Nullable HttpServer httpServer;
    private @Nullable HttpServer httpSslServer;
    private ServiceName instanceName;
    private RestAccessLogMode restAccessLogMode;

    public GrizzlyHttpService(
        Injector injector,
        ErrorReporter errorReporterRef,
        String listenAddressRef,
        String listenAddressSecureRef,
        @Nullable Path keyStoreFileRef,
        @Nullable String keyStorePasswordRef,
        @Nullable Path trustStoreFileRef,
        @Nullable String trustStorePasswordRef,
        @Nullable String restAccessLogPathRef,
        RestAccessLogMode restAccessLogModeRef,
        String webUiDirectoryRef
    )
    {
        sysCtx = injector.getInstance(Key.get(AccessContext.class, SystemContext.class));
        errorReporter = errorReporterRef;
        listenAddress = listenAddressRef;
        listenAddressSecure = listenAddressSecureRef;
        keyStoreFile = keyStoreFileRef;
        keyStorePassword = keyStorePasswordRef;
        trustStoreFile = trustStoreFileRef;
        trustStorePassword = trustStorePasswordRef;
        restAccessLogPath = Paths.get(restAccessLogPathRef);
        restAccessLogMode = restAccessLogModeRef;
        webUiDirectory = webUiDirectoryRef;
        restResourceConfig = new GuiceResourceConfig(injector).packages("com.linbit.linstor.api.rest");
        restResourceConfig.register(new CORSFilter());
        SystemConfRepository sysConfRepo = injector.getInstance(SystemConfRepository.class);
        AuthTokenRepository authTokenRepository = injector.getInstance(AuthTokenRepository.class);
        restResourceConfig.register(new AuthenticationFilter(authTokenRepository, sysConfRepo, sysCtx, errorReporter));
        registerExceptionMappers(restResourceConfig);
        lockGuardFactory = injector.getInstance(LockGuardFactory.class);
        systemConfRepository = injector.getInstance(SystemConfRepository.class);

        try
        {
            instanceName = new ServiceName("GrizzlyHttpServer");
        }
        catch (InvalidNameException ignored)
        {
        }
    }

    private static class HTTPSForwarder extends HttpHandler
    {
        private final int httpsPort;

        HTTPSForwarder(int httpsPortPrm)
        {
            httpsPort = httpsPortPrm;
        }

        @Override
        public void service(Request request, Response response) throws Exception
        {
            response.setStatus(HttpStatus.NOT_FOUND_404);
            response.sendRedirect(
                String.format("https://%s:%d", request.getServerName(), httpsPort) +
                    request.getRequestURI()
            );
        }
    }

    private void addHTTPSRedirectHandler(HttpServer httpServerRef, int httpsPort)
    {
        ArrayList<String> fwdMappings = new ArrayList<>();
        fwdMappings.add("/v1");

        boolean disableHttpMetrics = false;
        try (LockGuard ignored = lockGuardFactory.build(READ, CTRL_CONFIG))
        {
            disableHttpMetrics = systemConfRepository.getCtrlConfForView(sysCtx)
                .getPropWithDefault(ApiConsts.KEY_DISABLE_HTTP_METRICS, ApiConsts.NAMESPC_REST, "false")
                .equalsIgnoreCase("true");
        }
        catch (AccessDeniedException ignored)
        {
        }
        if (disableHttpMetrics)
        {
            fwdMappings.add("/metrics");
        }

        httpServerRef.getServerConfiguration().addHttpHandler(
            new HTTPSForwarder(httpsPort),
            fwdMappings.toArray(new String[0]));
    }

    private boolean isAutoHttpsEnabled()
    {
        try (LockGuard ignored = lockGuardFactory.build(READ, CTRL_CONFIG))
        {
            @Nullable String autoHttps = systemConfRepository
                .getCtrlConfForView(sysCtx)
                .getProp(ApiConsts.KEY_AUTO_HTTPS, ApiConsts.NAMESPC_REST);
            return Boolean.parseBoolean(autoHttps);
        }
        catch (AccessDeniedException ignored)
        {
            return false;
        }
    }

    private void addUiStaticHandler(HttpServer httpSrv)
    {
        httpSrv.getServerConfiguration().addHttpHandler(new StaticHttpHandler(webUiDirectory), "/ui");
    }

    private void enableCompression(HttpServer httpServerRef)
    {
        CompressionConfig compressionConfig = httpServerRef.getListener("grizzly").getCompressionConfig();
        compressionConfig.setCompressionMode(CompressionConfig.CompressionMode.ON);
        compressionConfig.setCompressibleMimeTypes("text/plain", "text/html", "application/json");
        compressionConfig.setCompressionMinSize(COMPRESSION_MIN_SIZE);
    }

    /**
     * Generates an in-memory KeyStore with a self-signed certificate for HTTPS using keytool.
     *
     * Modern java can't create in memory certificates, so the only way is to use keytool here.
     * To have everything pure in memory java, we would need a library like bouncycastle.
     */
    private byte[] generateSelfSignedKeyStore(String password) throws Exception
    {
        // Create a temporary keystore path (keytool needs a non-existing file)
        Path tempKeyStore = Files.createTempFile("linstor-keystore-", ".jks");
        Files.delete(tempKeyStore); // keytool requires the file to not exist
        try
        {
            // Use keytool to generate a self-signed certificate
            ProcessBuilder pb = new ProcessBuilder(
                "keytool",
                "-genkeypair",
                "-alias", "linstor",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "365",
                "-dname", "CN=LINSTOR Controller, O=LINBIT, C=AT",
                "-keystore", tempKeyStore.toString(),
                "-storepass", password,
                "-keypass", password,
                "-storetype", "PKCS12"
            );
            pb.inheritIO();
            Process process = pb.start();
            int exitCode = process.waitFor();
            if (exitCode != 0)
            {
                throw new IOException("keytool failed with exit code " + exitCode);
            }

            // Read the keystore into memory
            return Files.readAllBytes(tempKeyStore);
        }
        finally
        {
            Files.deleteIfExists(tempKeyStore);
        }
    }

    private void initGrizzly(final String bindAddress, final String httpsBindAddress)
    {
        if (keyStoreFile != null)
        {
            final URI httpsUri = URI.create(String.format("https://%s", httpsBindAddress));

            // only install a redirect handler for http
            httpServer = GrizzlyHttpServerFactory.createHttpServer(
                URI.create(String.format("http://%s", bindAddress)),
                restResourceConfig,
                false
            );

            addHTTPSRedirectHandler(httpServer, httpsUri.getPort());

            httpSslServer = GrizzlyHttpServerFactory.createHttpServer(
                httpsUri,
                restResourceConfig,
                false
            );

            SSLContextConfigurator sslCon = new SSLContextConfigurator();
            sslCon.setSecurityProtocol("TLS");
            sslCon.setKeyStoreFile(keyStoreFile.toString());
            sslCon.setKeyStorePass(keyStorePassword);

            boolean hasClientAuth = trustStoreFile != null;
            if (hasClientAuth)
            {
                sslCon.setTrustStoreFile(trustStoreFile.toString());
                sslCon.setTrustStorePass(trustStorePassword);
            }

            for (NetworkListener netListener : httpSslServer.getListeners())
            {
                netListener.setSecure(true);
                SSLEngineConfigurator ssle = new SSLEngineConfigurator(sslCon);
                ssle.setWantClientAuth(hasClientAuth);
                ssle.setClientMode(false);
                ssle.setNeedClientAuth(hasClientAuth);
                netListener.setSSLEngineConfig(ssle);
            }
        }
        else
        {
            // No keystore file configured - check if AutoHTTPS is enabled
            boolean autoHttpsEnabled = isAutoHttpsEnabled();

            if (autoHttpsEnabled)
            {
                // Generate self-signed certificate for HTTPS when AutoHTTPS is enabled
                final URI httpsUri = URI.create(String.format("https://%s", httpsBindAddress));
                String selfSignedPassword = "linstor";

                try
                {
                    byte[] keyStoreBytes;
                    if (Files.exists(AUTO_HTTPS_KEYSTORE_PATH))
                    {
                        keyStoreBytes = Files.readAllBytes(AUTO_HTTPS_KEYSTORE_PATH);
                        errorReporter.logInfo(
                            "Reusing existing AutoHTTPS keystore from %s",
                            AUTO_HTTPS_KEYSTORE_PATH
                        );
                    }
                    else
                    {
                        keyStoreBytes = generateSelfSignedKeyStore(selfSignedPassword);
                        Files.write(AUTO_HTTPS_KEYSTORE_PATH, keyStoreBytes);
                        errorReporter.logInfo(
                            "Generated new AutoHTTPS keystore and saved to %s",
                            AUTO_HTTPS_KEYSTORE_PATH
                        );
                    }

                    // Create HTTP server with redirect to HTTPS
                    httpServer = GrizzlyHttpServerFactory.createHttpServer(
                        URI.create(String.format("http://%s", bindAddress)),
                        restResourceConfig,
                        false
                    );
                    addHTTPSRedirectHandler(httpServer, httpsUri.getPort());

                    // Create HTTPS server with self-signed certificate
                    httpSslServer = GrizzlyHttpServerFactory.createHttpServer(
                        httpsUri,
                        restResourceConfig,
                        false
                    );

                    SSLContextConfigurator sslCon = new SSLContextConfigurator();
                    sslCon.setSecurityProtocol("TLS");
                    sslCon.setKeyStoreBytes(keyStoreBytes);
                    sslCon.setKeyStorePass(selfSignedPassword);

                    for (NetworkListener netListener : httpSslServer.getListeners())
                    {
                        netListener.setSecure(true);
                        SSLEngineConfigurator ssle = new SSLEngineConfigurator(sslCon);
                        ssle.setClientMode(false);
                        ssle.setNeedClientAuth(false);
                        netListener.setSSLEngineConfig(ssle);
                    }

                    errorReporter.logInfo(
                        "Using self-signed certificate for HTTPS on %s (not recommended for production)",
                        httpsBindAddress
                    );
                }
                catch (Exception exc)
                {
                    errorReporter.logWarning(
                        "Failed to generate self-signed certificate, falling back to HTTP only: %s",
                        exc.getMessage()
                    );
                    httpSslServer = null;
                    httpServer = GrizzlyHttpServerFactory.createHttpServer(
                        URI.create(String.format("http://%s", bindAddress)),
                        restResourceConfig,
                        false
                    );
                }
            }
            else
            {
                // No AutoHTTPS, no keystore - HTTP only
                httpServer = GrizzlyHttpServerFactory.createHttpServer(
                    URI.create(String.format("http://%s", bindAddress)),
                    restResourceConfig,
                    false
                );
                errorReporter.logInfo(
                    "Starting HTTP only on %s (no keystore configured and AutoHTTPS not enabled)",
                    bindAddress
                );
            }
        }

        if (restAccessLogMode != LinstorConfig.RestAccessLogMode.NO_LOG)
        {
            final Path accessLogPath = restAccessLogPath.isAbsolute() ?
                restAccessLogPath :
                errorReporter.getLogDirectory().resolve(restAccessLogPath);
            final AccessLogBuilder builder = new AccessLogBuilder(accessLogPath.toFile());

            switch (restAccessLogMode)
            {
                case ROTATE_HOURLY:
                    errorReporter.logDebug("Rest-access log set to rotate hourly.");
                    builder.rotatedHourly();
                    break;
                case ROTATE_DAILY:
                    errorReporter.logDebug("Rest-access log set to rotate daily.");
                    builder.rotatedDaily();
                    break;
                case APPEND:
                    // fall-through
                case NO_LOG:
                    // fall-through
                default:
                    break;
            }

            if (httpServer != null)
            {
                builder.instrument(httpServer.getServerConfiguration());
            }
            if (httpSslServer != null)
            {
                builder.instrument(httpSslServer.getServerConfiguration());
            }
        }
        else
        {
            errorReporter.logDebug("Rest-access log turned off.");
        }

        // there is either https (+ http for redirect) or just http
        // so we only need features enabled on the primary method
        if (httpSslServer != null)
        {
            enableFeatures(httpSslServer);
        }
        else
        {
            enableFeatures(httpServer);
        }
    }

    private void enableFeatures(HttpServer httpServerRef)
    {
        enableCompression(httpServerRef);
        addUiStaticHandler(httpServerRef);
        httpServerRef.getHttpHandler().setAllowEncodedSlash(true);
        httpServerRef.getServerConfiguration().setAllowPayloadForUndefinedHttpMethods(true);
    }

    private void registerExceptionMappers(ResourceConfig resourceConfig)
    {
        resourceConfig.register(new LinstorMapper(errorReporter));
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

            if (httpSslServer != null)
            {
                httpSslServer.start();
            }
        }
        catch (SocketException sexc)
        {
            if (!httpServer.isStarted())
            {
                errorReporter.logError("Unable to start grizzly http server on " + listenAddress + ".");
            }
            if (httpSslServer != null && !httpSslServer.isStarted())
            {
                errorReporter.logError("Unable to start grizzly https server on " + listenAddressSecure + ".");
            }
            errorReporter.reportError(sexc);
            // ipv6 failed, if it is localhost ipv6, retry ipv4
            if (listenAddress.startsWith("[::]"))
            {
                URI uri = URI.create(String.format("http://%s", listenAddress));
                URI uriSecure = URI.create(String.format("https://%s", listenAddressSecure));
                errorReporter.logInfo("Trying to start grizzly http server on fallback ipv4: 0.0.0.0");
                try
                {
                    initGrizzly("0.0.0.0:" + uri.getPort(), "0.0.0.0:" + uriSecure.getPort());
                    httpServer.start();

                    if (httpSslServer != null)
                    {
                        httpSslServer.start();
                    }
                }
                catch (IOException exc)
                {
                    throw new SystemServiceStartException(
                        "Unable to start grizzly http server on fallback ipv4",
                        exc,
                        true
                    );
                }
            }
        }
        catch (IOException exc)
        {
            throw new SystemServiceStartException("Unable to start grizzly http server", exc, true);
        }
    }

    @Override
    public void shutdown(boolean ignoredJvmShutdownRef)
    {
        if (httpServer != null)
        {
            httpServer.shutdownNow();
        }
        if (httpSslServer != null)
        {
            httpSslServer.shutdownNow();
        }
    }

    @SuppressWarnings("FutureReturnValueIgnored") // shutdown() already blocks for the given timeout
    @Override
    public void awaitShutdown(long timeout)
    {
        if (httpServer != null)
        {
            httpServer.shutdown(timeout, TimeUnit.SECONDS);
        }
        if (httpSslServer != null)
        {
            httpSslServer.shutdown(timeout, TimeUnit.SECONDS);
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
        if (svcName == null)
        {
            throw new ImplementationError("unable to create service-name Grizzly-HTTP-Server");
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
        return (httpServer != null && httpServer.isStarted()) ||
            (httpSslServer != null && httpSslServer.isStarted());
    }
}

@Provider
class LinstorMapper implements ExceptionMapper<Exception>
{
    private final ErrorReporter errorReporter;

    @Context private UriInfo uriInfo;
    @Context private javax.ws.rs.core.Request request;

    // uriInfo and request are @Context variables which are filled automatically, since sb does not realize this we
    // ignore the warning
    @SuppressFBWarnings("NP_NONNULL_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
    LinstorMapper(
        ErrorReporter errorReporterRef
    )
    {
        errorReporter = errorReporterRef;
    }

    @Override
    public javax.ws.rs.core.Response toResponse(Exception exc)
    {
        javax.ws.rs.core.Response.Status respStatus;

        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        if (exc instanceof ApiRcException apiRcException)
        {
            errorReporter.reportError(exc);
            apiCallRc.addEntries(apiRcException.getApiCallRc());
            respStatus = javax.ws.rs.core.Response.Status.BAD_REQUEST;
        }
        else
        if (exc instanceof JsonMappingException ||
            exc instanceof JsonParseException)
        {
            String errorReport = errorReporter.reportError(exc);
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
        if (exc instanceof NotFoundException)
        {
            final String msg = String.format("Path '/%s' not found on server.", uriInfo.getPath());
            errorReporter.logWarning(msg);
            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    msg)
                        .setDetails(exc.getMessage())
                        .setSkipErrorReport(true)
                        .build());
            respStatus = javax.ws.rs.core.Response.Status.NOT_FOUND;
        }
        else
        if (exc instanceof NotAllowedException)
        {
            final String msg = String.format("Method '%s' not allowed on path '/%s'.",
                request.getMethod(), uriInfo.getPath());
            errorReporter.logWarning(msg);
            apiCallRc.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    msg)
                    .setDetails(exc.getMessage())
                    .setSkipErrorReport(true)
                    .build());
            respStatus = javax.ws.rs.core.Response.Status.METHOD_NOT_ALLOWED;
        }
        else
        {
            String errorReport = errorReporter.reportError(exc);
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
            .entity(ApiCallRcRestUtils.toJSON(errorReporter, apiCallRc))
            .build();
    }
}
