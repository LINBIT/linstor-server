package com.linbit.linstor.api.rest.v1.config;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.cfg.LinstorConfig.RestAccessLogMode;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.AuthTokenRepository;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.locks.LockGuardFactory.LockObj.CTRL_CONFIG;
import static com.linbit.locks.LockGuardFactory.LockType.READ;

import jakarta.ws.rs.NotAllowedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.inject.Injector;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.http.CompressionConfig;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.HttpServerFilter;
import org.glassfish.grizzly.http.server.HttpServerProbe;
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
import org.glassfish.jersey.server.ServerProperties;

public class GrizzlyHttpService implements SystemService
{
    private static final int COMPRESSION_MIN_SIZE = 1000; // didn't find a good default, so lets say 1000
    private static final Path AUTO_HTTPS_KEYSTORE_PATH = Paths.get("/var/lib/linstor/autohttps-keystore.p12");
    private static final Duration KEYSTORE_RENEW_MARGIN = Duration.ofDays(90);
    private static final int GENERAL_NAME_DNS = 2;
    private static final int GENERAL_NAME_IP = 7;

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
    private final NodeRepository nodeRepository;
    private final LockGuardFactory lockGuardFactory;
    private final String webUiDirectory;

    private @Nullable HttpServer httpServer;
    private @Nullable HttpServer httpSslServer;
    public static final ServiceName INSTANCE_NAME;
    static
    {
        try
        {
            INSTANCE_NAME = new ServiceName("GrizzlyHttpServer");
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError("Invalid service name", exc);
        }
    }
    private final RestAccessLogMode restAccessLogMode;

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
        // no JAXB implementation on the classpath; avoid the "WADL feature is disabled" warning at startup
        restResourceConfig.property(ServerProperties.WADL_FEATURE_DISABLE, true);
        restResourceConfig.register(new CORSFilter());
        SystemConfRepository sysConfRepo = injector.getInstance(SystemConfRepository.class);
        AuthTokenRepository authTokenRepository = injector.getInstance(AuthTokenRepository.class);
        restResourceConfig.register(new AuthenticationFilter(authTokenRepository, sysConfRepo, errorReporter));
        registerExceptionMappers(restResourceConfig);
        lockGuardFactory = injector.getInstance(LockGuardFactory.class);
        systemConfRepository = injector.getInstance(SystemConfRepository.class);
        nodeRepository = injector.getInstance(NodeRepository.class);
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
        fwdMappings.add("/ui");

        boolean disableHttpMetrics = false;
        try (LockGuard ignored = lockGuardFactory.build(READ, CTRL_CONFIG))
        {
            disableHttpMetrics = systemConfRepository.getCtrlConfForView()
                .getPropWithDefault(ApiConsts.KEY_DISABLE_HTTP_METRICS, ApiConsts.NAMESPC_REST, "false")
                .equalsIgnoreCase("true");
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
                .getCtrlConfForView()
                .getProp(ApiConsts.KEY_AUTO_HTTPS, ApiConsts.NAMESPC_REST);
            return Boolean.parseBoolean(autoHttps);
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
            // SubjectAlternativeName covering every address a client might use to
            // reach the controller (see buildSubjectAltNames).
            String subjectAltNames = buildSubjectAltNames();

            // Use keytool to generate a self-signed certificate
            ProcessBuilder pb = new ProcessBuilder(
                "keytool",
                "-genkeypair",
                "-alias", "linstor",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "1825",
                "-dname", "CN=LINSTOR Controller, O=LINBIT, C=AT",
                "-ext", "san=" + subjectAltNames,
                "-ext", "eku=serverAuth",
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

    /**
     * Builds the keytool "san=" value used when generating the certificate: the
     * cluster-wide required names (see requiredSubjectAltNames) plus the running
     * node's own local interface addresses and hostname(s). The local addresses
     * help reach this specific node but are deliberately left out of the reuse
     * decision, so an HA failover to a peer does not force a regeneration. An
     * address the controller cannot learn at all, such as an external NAT or VIP
     * address, still needs an operator-provided keystore (the [https] keystore
     * option).
     */
    private String buildSubjectAltNames()
    {
        LinkedHashSet<String> sans = new LinkedHashSet<>(requiredSubjectAltNames());

        try
        {
            InetAddress localHost = InetAddress.getLocalHost();
            for (String name : new String[] {localHost.getCanonicalHostName(), localHost.getHostName()})
            {
                if (name != null && !name.isEmpty() && !isIpLiteral(name))
                {
                    sans.add("dns:" + name);
                }
            }
        }
        catch (UnknownHostException ignored)
        {
            // best-effort: no resolvable local hostname
        }

        try
        {
            Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
            while (ifaces != null && ifaces.hasMoreElements())
            {
                Enumeration<InetAddress> addrs = ifaces.nextElement().getInetAddresses();
                while (addrs.hasMoreElements())
                {
                    InetAddress addr = addrs.nextElement();
                    // loopback is in the required set; link-local/scoped are not useful in a cert
                    if (!addr.isLoopbackAddress() && !addr.isLinkLocalAddress() && !addr.isMulticastAddress())
                    {
                        String ip = addr.getHostAddress();
                        int scope = ip.indexOf('%');
                        if (scope >= 0)
                        {
                            ip = ip.substring(0, scope);
                        }
                        sans.add("ip:" + ip);
                    }
                }
            }
        }
        catch (SocketException ignored)
        {
            // best-effort: could not enumerate interfaces
        }

        return String.join(",", sans);
    }

    /**
     * The cluster-wide SubjectAlternativeName entries the certificate must cover:
     * localhost plus every controller-capable node's name and registered
     * addresses from the node database. These are the addresses a client uses to
     * reach any controller and are identical on every node, so a certificate that
     * covers them can be reused across an HA failover instead of regenerated.
     * Returned as "ip:x"/"dns:y" tokens. Best-effort: if the node database is not
     * readable yet, only localhost is returned.
     */
    private LinkedHashSet<String> requiredSubjectAltNames()
    {
        LinkedHashSet<String> sans = new LinkedHashSet<>();
        sans.add("dns:localhost");
        sans.add("ip:127.0.0.1");
        sans.add("ip:::1");

        for (Node node : nodeRepository.getMapForView().values())
        {
            Node.Type nodeType = node.getNodeType();
            if (nodeType == Node.Type.CONTROLLER || nodeType == Node.Type.COMBINED)
            {
                String nodeName = node.getName().displayValue;
                if (nodeName != null && !nodeName.isEmpty() && !isIpLiteral(nodeName))
                {
                    sans.add("dns:" + nodeName);
                }
                Iterator<NetInterface> netIfs = node.iterateNetInterfaces();
                while (netIfs.hasNext())
                {
                    String addr = netIfs.next().getAddress().getAddress();
                    if (addr != null && !addr.isEmpty())
                    {
                        sans.add((isIpLiteral(addr) ? "ip:" : "dns:") + addr);
                    }
                }
            }
        }

        return sans;
    }

    private static boolean isIpLiteral(String host)
    {
        // Do not emit a dns: entry for a name that is really an IP literal
        return host.indexOf(':') >= 0 || host.matches("^[0-9.]+$");
    }

    /**
     * Returns true if the persisted keystore can be reused: its certificate is
     * not expired or close to expiring, and it covers the cluster-wide required
     * SubjectAlternativeNames (see requiredSubjectAltNames). Otherwise it is
     * regenerated, for example after a new controller node was added, or as the
     * certificate nears its validity end - a restart alone would otherwise reuse
     * it indefinitely. The per-node local addresses are excluded from the SAN
     * check so an HA failover to a peer reuses the certificate instead of
     * regenerating it. If the keystore cannot be read, returns false so it gets
     * regenerated.
     */
    private boolean canReuseKeystore(byte[] keystoreBytes, String password)
    {
        boolean reuse = false;
        try
        {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(new ByteArrayInputStream(keystoreBytes), password.toCharArray());
            Certificate cert = ks.getCertificate("linstor");
            // Regenerate before the certificate expires; a restart alone reuses it.
            if (cert instanceof X509Certificate x509 &&
                x509.getNotAfter().toInstant().isAfter(Instant.now().plus(KEYSTORE_RENEW_MARGIN)))
            {
                Set<String> present = new HashSet<>();
                Collection<List<?>> certSans = x509.getSubjectAlternativeNames();
                if (certSans != null)
                {
                    for (List<?> entry : certSans)
                    {
                        // entry is [Integer generalNameType, value]
                        if (entry.size() >= 2 &&
                            entry.get(0) instanceof Integer generalNameType &&
                            entry.get(1) instanceof String value)
                        {
                            present.add(normalizeSanValue(generalNameType, value));
                        }
                    }
                }
                reuse = present.containsAll(sanValues(requiredSubjectAltNames()));
            }
        }
        catch (Exception exc)
        {
            errorReporter.logWarning(
                "Could not inspect the existing AutoHTTPS keystore, regenerating it: %s",
                exc.getMessage()
            );
        }
        return reuse;
    }

    /**
     * Normalizes "ip:x"/"dns:y" tokens into a set for comparison against a
     * certificate's SubjectAlternativeName entries.
     */
    private Set<String> sanValues(Collection<String> tokens)
    {
        Set<String> out = new HashSet<>();
        for (String token : tokens)
        {
            int colon = token.indexOf(':');
            if (colon >= 0)
            {
                int type = token.substring(0, colon).equalsIgnoreCase("ip") ? GENERAL_NAME_IP : GENERAL_NAME_DNS;
                out.add(normalizeSanValue(type, token.substring(colon + 1)));
            }
        }
        return out;
    }

    /**
     * Normalizes a SAN value so the two sides compare equal regardless of form.
     * IP addresses are canonicalized through InetAddress (so ::1 and its expanded
     * form match); names are lower-cased.
     */
    private static String normalizeSanValue(int generalNameType, String value)
    {
        String normalized = null;
        if (generalNameType == GENERAL_NAME_IP)
        {
            try
            {
                normalized = "ip:" + InetAddress.getByName(value).getHostAddress();
            }
            catch (UnknownHostException ignored)
            {
                // not a parseable literal, fall through to a plain compare
            }
        }
        if (normalized == null)
        {
            normalized = "dns:" + value.toLowerCase(Locale.ROOT);
        }
        return normalized;
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
                    byte[] existingKeyStore = Files.exists(AUTO_HTTPS_KEYSTORE_PATH) ?
                        Files.readAllBytes(AUTO_HTTPS_KEYSTORE_PATH) : null;
                    if (existingKeyStore != null && canReuseKeystore(existingKeyStore, selfSignedPassword))
                    {
                        keyStoreBytes = existingKeyStore;
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
                            existingKeyStore == null ?
                                "Generated new AutoHTTPS keystore and saved to %s" :
                                "Regenerated the AutoHTTPS keystore at %s because the existing certificate" +
                                    " no longer covered all controller addresses or was near expiry",
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

        if (restAccessLogMode != RestAccessLogMode.NO_LOG)
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

    @SuppressWarnings("rawtypes")
    private void installRemoteAddrProbe(HttpServer httpServerRef)
    {
        httpServerRef.getServerConfiguration().getMonitoringConfig().getWebServerConfig()
            .addProbes(new HttpServerProbe.Adapter()
            {
                @Override
                public void onRequestReceiveEvent(
                    HttpServerFilter filter,
                    Connection connection,
                    Request request
                )
                {
                    request.setAttribute(
                        AuthenticationFilter.REMOTE_ADDR_PROPERTY,
                        request.getRemoteAddr()
                    );
                }
            });
    }

    private void enableFeatures(HttpServer httpServerRef)
    {
        enableCompression(httpServerRef);
        addUiStaticHandler(httpServerRef);
        httpServerRef.getHttpHandler().setAllowEncodedSlash(true);
        httpServerRef.getServerConfiguration().setAllowPayloadForUndefinedHttpMethods(true);
        installRemoteAddrProbe(httpServerRef);
    }

    private void registerExceptionMappers(ResourceConfig resourceConfig)
    {
        resourceConfig.register(new LinstorMapper(errorReporter));
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceNameRef)
    {
        // instance name is static final, ignore
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

    public void restart()
    {
        errorReporter.logInfo("Restarting Grizzly HTTP service to apply new configuration");
        shutdown(false);
        try
        {
            start();
        }
        catch (SystemServiceStartException exc)
        {
            errorReporter.reportError(exc);
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
        return INSTANCE_NAME;
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
    @Context private jakarta.ws.rs.core.Request request;

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
    public jakarta.ws.rs.core.Response toResponse(Exception exc)
    {
        jakarta.ws.rs.core.Response.Status respStatus;

        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        if (exc instanceof ApiRcException apiRcException)
        {
            errorReporter.reportError(exc);
            apiCallRc.addEntries(apiRcException.getApiCallRc());
            respStatus = jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
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
            respStatus = jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
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
            respStatus = jakarta.ws.rs.core.Response.Status.NOT_FOUND;
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
            respStatus = jakarta.ws.rs.core.Response.Status.METHOD_NOT_ALLOWED;
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
            respStatus = jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
        }

        return jakarta.ws.rs.core.Response
            .status(respStatus)
            .type(MediaType.APPLICATION_JSON)
            .entity(ApiCallRcRestUtils.toJSON(errorReporter, apiCallRc))
            .build();
    }
}
