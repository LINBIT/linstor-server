package com.linbit.linstor.core;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.linbit.ImplementationError;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.protobuf.ProtobufApiType;
import com.linbit.linstor.debug.DebugConsole;
import com.linbit.linstor.debug.DebugConsoleCreator;
import com.linbit.linstor.debug.DebugConsoleImpl;
import com.linbit.linstor.drbdstate.DrbdEventService;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.netcom.TcpConnectorService;
import com.linbit.linstor.netcom.ssl.SslTcpConnectorService;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.CommonMessageProcessor;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Initializer;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.timer.CoreTimer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * linstor satellite prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Satellite extends LinStor
{
    // System module information
    public static final String MODULE = "Satellite";

    public static final String NET_COM_DEFAULT_TYPE = "plain";
    public static final String NET_COM_DEFAULT_ADDR = "::0";
    public static final int NET_COM_DEFAULT_PORT = 3366;

    // TCP Service configuration file
    public static final String NET_COM_CONF_FILE = "satellite_netcom.cfg";
    // Plain TCP Service configuration keys
    public static final String NET_COM_CONF_TYPE_KEY = "type";
    public static final String NET_COM_CONF_BIND_ADDR_KEY = "bind-address";
    public static final String NET_COM_CONF_PORT_KEY = "port";
    public static final String NET_COM_CONF_TYPE_PLAIN = "plain";
    public static final String NET_COM_CONF_TYPE_SSL = "ssl";
    // SSL Service configuration keys
    public static final String NET_COM_CONF_SSL_SERVER_CERT_KEY = "server-certificate";
    public static final String NET_COM_CONF_SSL_TRUST_CERT_KEY = "trusted-certificates";
    public static final String NET_COM_CONF_SSL_KEY_PASS_KEY = "key-passwd";
    public static final String NET_COM_CONF_SSL_KEYSTORE_PASS_KEY = "keystore-passwd";
    public static final String NET_COM_CONF_SSL_TRUST_PASS_KEY = "truststore-passwd";
    public static final String NET_COM_CONF_SSL_PROTOCOL_KEY = "ssl-protocol";

    private final Injector injector;

    // System security context
    private AccessContext sysCtx;

    // Public security context
    private AccessContext publicCtx;

    private StltApiCallHandler apiCallHandler;

    // ============================================================
    // Message processing dispatcher
    //
    private CommonMessageProcessor msgProc;

    // ============================================================
    // Core system services
    //
    // Map of controllable system services
    private Map<ServiceName, SystemService> systemServicesMap;

    // Map of connected peers
    private CoreModule.PeerMap peerMap;

    // Map of network communications connectors
    private final Map<ServiceName, TcpConnector> netComConnectors;

    // Device manager
    private DeviceManagerImpl devMgr = null;

    private ApplicationLifecycleManager applicationLifecycleManager;

    // Lock for major global changes
    public ReadWriteLock stltConfLock;

    private UpdateMonitor updateMonitor;

    private DebugConsoleCreator debugConsoleCreator;

    public Satellite(
        Injector injectorRef,
        AccessContext sysCtxRef,
        AccessContext publicCtxRef
    )
    {
        injector = injectorRef;

        // Initialize security contexts
        sysCtx = sysCtxRef;
        publicCtx = publicCtxRef;

        // Initialize network communications connectors map
        netComConnectors = new TreeMap<>();
    }

    public void initialize()
    {
        reconfigurationLock = injector.getInstance(
            Key.get(ReadWriteLock.class, Names.named(CoreModule.RECONFIGURATION_LOCK)));
        nodesMapLock = injector.getInstance(
            Key.get(ReadWriteLock.class, Names.named(CoreModule.NODES_MAP_LOCK)));
        rscDfnMapLock = injector.getInstance(
            Key.get(ReadWriteLock.class, Names.named(CoreModule.RSC_DFN_MAP_LOCK)));
        storPoolDfnMapLock = injector.getInstance(
            Key.get(ReadWriteLock.class, Names.named(CoreModule.STOR_POOL_DFN_MAP_LOCK)));
        stltConfLock = injector.getInstance(
            Key.get(ReadWriteLock.class, Names.named(SatelliteCoreModule.STLT_CONF_LOCK)));

        reconfigurationLock.writeLock().lock();

        try
        {
            AccessContext initCtx = sysCtx.clone();
            initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            errorReporter = injector.getInstance(ErrorReporter.class);

            systemServicesMap = injector.getInstance(Key.get(new TypeLiteral<Map<ServiceName, SystemService>>() {}));

            FileSystemWatch fsWatchSvc = injector.getInstance(FileSystemWatch.class);
            systemServicesMap.put(fsWatchSvc.getInstanceName(), fsWatchSvc);

            timerEventSvc = injector.getInstance(CoreTimer.class);
            systemServicesMap.put(timerEventSvc.getInstanceName(), timerEventSvc);

            peerMap = injector.getInstance(CoreModule.PeerMap.class);

            applicationLifecycleManager = injector.getInstance(ApplicationLifecycleManager.class);

            updateMonitor = injector.getInstance(UpdateMonitor.class);

            apiCallHandler = injector.getInstance(StltApiCallHandler.class);

            debugConsoleCreator = injector.getInstance(DebugConsoleCreator.class);

            // Initialize the message processor
            // errorLogRef.logInfo("Initializing API call dispatcher");
            msgProc = injector.getInstance(CommonMessageProcessor.class);

            errorReporter.logInfo("Initializing StateTracker");
            {
                DrbdEventService drbdEventSvc = injector.getInstance(DrbdEventService.class);

                systemServicesMap.put(drbdEventSvc.getInstanceName(), drbdEventSvc);
            }

            errorReporter.logInfo("Initializing device manager");
            devMgr = injector.getInstance(DeviceManagerImpl.class);
            systemServicesMap.put(devMgr.getInstanceName(), devMgr);

            // Initialize system services
            applicationLifecycleManager.startSystemServices(systemServicesMap.values());

            // Initialize the network communications service
            errorReporter.logInfo("Initializing main network communications service");
            initMainNetComService(initCtx);
        }
        catch (AccessDeniedException accessExc)
        {
            throw new ImplementationError(
                "The initialization security context does not have all privileges. " +
                    "Initialization failed.",
                accessExc
            );
        }
        finally
        {
            reconfigurationLock.writeLock().unlock();
        }
    }

    private void enterDebugConsole()
    {
        try
        {
            errorReporter.logInfo("Entering debug console");

            AccessContext privCtx = sysCtx.clone();
            AccessContext debugCtx = sysCtx.clone();
            privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
            debugCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            DebugConsole dbgConsole = debugConsoleCreator.createDebugConsole(privCtx, debugCtx, null);
            dbgConsole.stdStreamsConsole(DebugConsoleImpl.CONSOLE_PROMPT);
            System.out.println();

            errorReporter.logInfo("Debug console exited");
        }
        catch (Throwable error)
        {
            errorReporter.reportError(error);
        }

        try
        {
            AccessContext shutdownCtx = sysCtx.clone();
            // Just in case that someone removed the access control list entry
            // for the system's role or changed the security type for shutdown,
            // override access controls with the system context's privileges
            shutdownCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_OBJ_USE, Privilege.PRIV_MAC_OVRD);
            applicationLifecycleManager.shutdown(shutdownCtx);
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "Cannot shutdown() using the system's security context. " +
                "Suspected removal of privileges from the system context.",
                accExc
            );
        }
    }

    private void initMainNetComService(AccessContext initCtx)
    {
        try
        {
            Properties netComProps = new Properties();
            if (Files.exists(Paths.get(NET_COM_CONF_FILE)))
            {
                try (InputStream propsIn = new FileInputStream(NET_COM_CONF_FILE))
                {
                    netComProps.loadFromXML(propsIn);
                }
                catch (FileNotFoundException fileExc)
                {
                    // this should never happen due to the if (Files.exists(...)), but if it
                    // still happens it can be ignored, as every property has a default-value
                }
            }
            InetAddress addr = InetAddress.getByName(
                netComProps.getProperty(
                    NET_COM_CONF_BIND_ADDR_KEY,
                    NET_COM_DEFAULT_ADDR
                )
            );
            String portProp = netComProps.getProperty(
                NET_COM_CONF_PORT_KEY,
                Integer.toString(NET_COM_DEFAULT_PORT)
            );
            int port = Integer.parseInt(portProp);
            SocketAddress bindAddress = new InetSocketAddress(addr, port);

            TcpConnector netComSvc = null;

            String type = netComProps.getProperty(NET_COM_CONF_TYPE_KEY, NET_COM_DEFAULT_TYPE);
            if (type.equalsIgnoreCase(NET_COM_CONF_TYPE_PLAIN))
            {
                netComSvc = new TcpConnectorService(
                    errorReporter,
                    msgProc,
                    bindAddress,
                    publicCtx,
                    initCtx,
                    new StltConnTracker(peerMap)
                );
            }
            else
            if (type.equalsIgnoreCase(NET_COM_CONF_TYPE_SSL))
            {
                String sslProtocol = netComProps.getProperty(NET_COM_CONF_SSL_PROTOCOL_KEY);
                String keyStoreFile = netComProps.getProperty(NET_COM_CONF_SSL_SERVER_CERT_KEY);
                String trustStoreFile = netComProps.getProperty(NET_COM_CONF_SSL_TRUST_CERT_KEY);
                char[] keyPasswd = netComProps.getProperty(NET_COM_CONF_SSL_KEY_PASS_KEY).toCharArray();
                char[] keyStorePasswd = netComProps.getProperty(NET_COM_CONF_SSL_KEYSTORE_PASS_KEY).toCharArray();
                char[] trustStorePasswd = netComProps.getProperty(NET_COM_CONF_SSL_TRUST_PASS_KEY).toCharArray();

                try
                {
                    netComSvc = new SslTcpConnectorService(
                        errorReporter,
                        msgProc,
                        bindAddress,
                        publicCtx,
                        initCtx,
                        new StltConnTracker(peerMap),
                        sslProtocol,
                        keyStoreFile,
                        keyStorePasswd,
                        keyPasswd,
                        trustStoreFile,
                        trustStorePasswd
                    );
                }
                catch (KeyManagementException keyMgmtExc)
                {
                    errorReporter.reportError(
                        new LinStorException(
                            "Initialization of the SSLContext failed. See cause for details",
                            keyMgmtExc
                        )
                    );
                }
                catch (UnrecoverableKeyException unrecoverableKeyExc)
                {
                    String errorMsg = "A private or public key for the initialization of SSL encryption could " +
                        "not be loaded";
                    errorReporter.reportError(
                        new LinStorException(
                            errorMsg,
                            errorMsg,
                            null,
                            "Check whether the password for the SSL keystores is correct.",
                            null,
                            unrecoverableKeyExc
                        )
                    );
                }
                catch (NoSuchAlgorithmException exc)
                {
                    errorReporter.reportError(
                        new LinStorException(
                            String.format(
                                "SSL initialization failed: " +
                                "The SSL/TLS encryption protocol '%s' is not available on this system.",
                                sslProtocol
                            ),
                            "SSL initialization failed.",
                            String.format(
                                "The SSL/TLS protocol '%s' is not available on this system",
                                sslProtocol
                            ),
                            "- Select a supported SSL/TLS protocol in the network communications configuration\n" +
                            "or\n" +
                            "- Enable support for the currently selected SSL/TLS protocol on this system",
                            null,
                            exc
                        )
                    );
                }
                catch (KeyStoreException keyStoreExc)
                {
                    throw new ImplementationError(
                        "Default SSL keystore type could not be found by the KeyStore instance",
                        keyStoreExc
                    );
                }
                catch (CertificateException exc)
                {
                    errorReporter.reportError(
                        new LinStorException(
                            "A required SSL certificate could not be loaded",
                            "A required SSL certificate could not be loaded from the keystore files",
                            null,
                            "Ensure that the required SSL certificates are contained in the keystore files.\n" +
                            "Refer to documentation for information on how to setup SSL encryption.",
                            null,
                            exc
                        )
                    );
                }
            }

            if (netComSvc != null)
            {
                try
                {
                    netComConnectors.put(netComSvc.getInstanceName(), netComSvc);
                    systemServicesMap.put(netComSvc.getInstanceName(), netComSvc);
                    netComSvc.start();
                    errorReporter.logInfo(
                        String.format(
                            "%s started on port %s:%d",
                            netComSvc.getInstanceName().displayValue,
                            addr, port
                        )
                    );
                }
                catch (SystemServiceStartException sysSvcStartExc)
                {
                    String errorMsg = sysSvcStartExc.getMessage();
                    if (errorMsg == null)
                    {
                        errorMsg = "The initial network communications service failed to start.";
                    }
                    errorReporter.reportError(
                        new LinStorException(
                            errorMsg,
                            errorMsg, // description
                            null, // cause
                            null, // correction
                            null, // details
                            sysSvcStartExc // Nested throwable
                        )
                    );

                }
            }
            else
            {
                if (!NET_COM_CONF_TYPE_PLAIN.equalsIgnoreCase(type) &&
                    !NET_COM_CONF_TYPE_SSL.equalsIgnoreCase(type))
                {
                    errorReporter.reportError(
                        new LinStorException(
                            // Message
                            String.format(
                                "The property '%s' in configuration file '%s' is misconfigured",
                                NET_COM_CONF_TYPE_KEY, NET_COM_CONF_FILE
                            ),
                            // Description
                            "The initial network communication service can not be started.",
                            // Cause
                            String.format(
                                "The service type is misconfigured.\n" +
                                "The property '%s' must be either '%s' or '%s', but was '%s'",
                                NET_COM_CONF_TYPE_KEY,
                                NET_COM_CONF_TYPE_PLAIN,
                                NET_COM_CONF_TYPE_SSL,
                                type
                            ),
                            // Error details
                            String.format(
                                "The network communication service configuration file is:\n%s",
                                NET_COM_CONF_FILE
                            ),
                            // No nested exception
                            null
                        )
                    );
                }
            }
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
        }
    }

    public static void main(String[] args)
    {
        LinStorArguments cArgs = LinStorArgumentParser.parseCommandLine(args);

        System.out.printf(
            "%s, Module %s\n",
            Satellite.PROGRAM, Satellite.MODULE
        );
        printStartupInfo();

        ErrorReporter errorLog = new StdErrorReporter(Satellite.MODULE, "");

        try
        {
            Thread.currentThread().setName("Main");

            ApiType apiType = new ProtobufApiType();
            List<Class<? extends ApiCall>> apiCalls =
                new ApiCallLoader(errorLog).loadApiCalls(apiType, Arrays.asList("common", "satellite"));

            // Initialize the Satellite module with the SYSTEM security context
            Initializer sysInit = new Initializer();
            Satellite instance = sysInit.initSatellite(cArgs, errorLog, apiType, apiCalls);

            instance.initialize();
            if (cArgs.startDebugConsole())
            {
                instance.enterDebugConsole();
            }
        }
        catch (Throwable error)
        {
            errorLog.reportError(error);
        }

        System.out.println();
    }
}
