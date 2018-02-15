package com.linbit.linstor.core;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.linbit.ImplementationError;
import com.linbit.SatelliteLinbitModule;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.WorkerPool;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.CoreServices;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SatelliteDbDriver;
import com.linbit.linstor.SatellitePeerCtx;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.debug.DebugConsole;
import com.linbit.linstor.drbdstate.DrbdEventService;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.netcom.TcpConnectorService;
import com.linbit.linstor.netcom.ssl.SslTcpConnectorService;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.CommonMessageProcessor;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.EmptySecurityDbDriver;
import com.linbit.linstor.security.Initializer;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SecurityLevel;
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
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * linstor satellite prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Satellite extends LinStor implements CoreServices
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
    // Worker thread pool & message processing dispatcher
    //
    private WorkerPool workerThrPool = null;
    private CommonMessageProcessor msgProc;

    // ============================================================
    // Worker pool for satellite services operations - DeviceManager, etc.
    //
    private WorkerPool stltThrPool = null;

    // ============================================================
    // Core system services
    //
    // Map of controllable system services
    private Map<ServiceName, SystemService> systemServicesMap;

    // Map of connected peers
    private Map<String, Peer> peerMap;

    // Map of network communications connectors
    private final Map<ServiceName, TcpConnector> netComConnectors;

    // The current API type (e.g ProtoBuf)
    private final ApiType apiType;

    // Satellite configuration properties
    Props stltConf;

    // Map of all managed nodes
    Map<NodeName, Node> nodesMap;

    // Map of all resource definitions
    Map<ResourceName, ResourceDefinition> rscDfnMap;

    // Map of all storage pools
    Map<StorPoolName, StorPoolDefinition> storPoolDfnMap;

    private ControllerPeerConnector controllerPeerConnector;

    // Device manager
    private DeviceManagerImpl devMgr = null;

    private ApplicationLifecycleManager applicationLifecycleManager;

    // Lock for major global changes
    public ReadWriteLock stltConfLock;

    private UpdateMonitor updateMonitor;

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

        apiType = ApiType.PROTOBUF;
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

            ErrorReporter errorLogRef = injector.getInstance(ErrorReporter.class);

            // Initialize the error & exception reporting facility
            setErrorLog(initCtx, errorLogRef);

            systemServicesMap = injector.getInstance(Key.get(new TypeLiteral<Map<ServiceName, SystemService>>() {}));

            FileSystemWatch fsWatchSvc = injector.getInstance(FileSystemWatch.class);
            systemServicesMap.put(fsWatchSvc.getInstanceName(), fsWatchSvc);

            timerEventSvc = injector.getInstance(CoreTimer.class);
            {
                CoreTimer timer = super.getTimer();
                systemServicesMap.put(timer.getInstanceName(), timer);
            }

            // Initialize LinStor objects maps
            peerMap = injector.getInstance(CoreModule.PeerMap.class);
            nodesMap = injector.getInstance(CoreModule.NodesMap.class);
            rscDfnMap = injector.getInstance(CoreModule.ResourceDefinitionMap.class);
            storPoolDfnMap = injector.getInstance(CoreModule.StorPoolDefinitionMap.class);

            // initialize noop databases drivers (needed for shutdownProt)
            securityDbDriver = injector.getInstance(EmptySecurityDbDriver.class);
            persistenceDbDriver = injector.getInstance(SatelliteDbDriver.class);

            applicationLifecycleManager = injector.getInstance(ApplicationLifecycleManager.class);

            updateMonitor = injector.getInstance(UpdateMonitor.class);

            controllerPeerConnector = injector.getInstance(ControllerPeerConnector.class);

            // Initialize the worker thread pool
            workerThrPool = injector.getInstance(
                Key.get(WorkerPool.class, Names.named(SatelliteLinbitModule.MAIN_WORKER_POOL_NAME))
            );

            // Initialize the thread pool for satellite services operations
            stltThrPool = injector.getInstance(
                Key.get(WorkerPool.class, Names.named(SatelliteLinbitModule.STLT_WORKER_POOL_NAME))
            );

            apiCallHandler = injector.getInstance(StltApiCallHandler.class);

            // Initialize the message processor
            // errorLogRef.logInfo("Initializing API call dispatcher");
            msgProc = new CommonMessageProcessor(errorLogRef, workerThrPool);

            errorLogRef.logInfo("Initializing test APIs");
            LinStor.loadApiCalls(msgProc, this, this, apiType);


            errorLogRef.logInfo("Initializing StateTracker");
            {
                DrbdEventService drbdEventSvc = injector.getInstance(DrbdEventService.class);

                systemServicesMap.put(drbdEventSvc.getInstanceName(), drbdEventSvc);
            }

            errorLogRef.logInfo("Initializing device manager");
            devMgr = injector.getInstance(DeviceManagerImpl.class);
            systemServicesMap.put(devMgr.getInstanceName(), devMgr);

            // Initialize system services
            applicationLifecycleManager.startSystemServices(systemServicesMap.values());

            // Initialize the network communications service
            errorLogRef.logInfo("Initializing main network communications service");
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

    @Override
    public void setSecurityLevel(AccessContext accCtx, SecurityLevel newLevel)
        throws AccessDeniedException, SQLException
    {
        SecurityLevel.set(accCtx, newLevel, null, null);
    }

    public void enterDebugConsole()
    {
        ErrorReporter errLog = getErrorReporter();
        try
        {
            errLog.logInfo("Entering debug console");

            AccessContext privCtx = sysCtx.clone();
            AccessContext debugCtx = sysCtx.clone();
            privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
            debugCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            DebugConsole dbgConsole = createDebugConsole(privCtx, debugCtx, null);
            dbgConsole.stdStreamsConsole(StltDebugConsoleImpl.CONSOLE_PROMPT);
            System.out.println();

            errLog.logInfo("Debug console exited");
        }
        catch (Throwable error)
        {
            getErrorReporter().reportError(error);
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

    public void shutdown(AccessContext accCtx) throws AccessDeniedException
    {
        applicationLifecycleManager.shutdown(accCtx);
    }

    /**
     * Creates a debug console instance for remote use by a connected peer
     *
     * @param accCtx The access context to authorize this API call
     * @param client Connected peer
     * @return New DebugConsole instance
     * @throws AccessDeniedException If the API call is not authorized
     */
    public DebugConsole createDebugConsole(
        AccessContext accCtx,
        AccessContext debugCtx,
        Peer client
    )
        throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
        StltDebugConsoleImpl peerDbgConsole = new StltDebugConsoleImpl(
            this,
            debugCtx,
            systemServicesMap,
            peerMap,
            msgProc
        );
        if (client != null)
        {
            SatellitePeerCtx peerContext = (SatellitePeerCtx) client.getAttachment();
            // Initialize remote debug console
            // FIXME: loadDefaultCommands() should not use System.out and System.err
            //        if the debug console is created for a peer / client
            peerDbgConsole.loadDefaultCommands(System.out, System.err);
            peerContext.setDebugConsole(peerDbgConsole);
        }
        else
        {
            // Initialize local debug console
            peerDbgConsole.loadDefaultCommands(System.out, System.err);
        }

        return peerDbgConsole;
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
                    this,
                    msgProc,
                    bindAddress,
                    publicCtx,
                    initCtx,
                    new StltConnTracker(this, peerMap)
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
                        this,
                        msgProc,
                        bindAddress,
                        publicCtx,
                        initCtx,
                        new StltConnTracker(this, peerMap),
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
                    getErrorReporter().reportError(
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
                    getErrorReporter().reportError(
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
                    getErrorReporter().reportError(
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
                    getErrorReporter().reportError(
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
                    getErrorReporter().logInfo(
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
                    getErrorReporter().reportError(
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
                    getErrorReporter().reportError(
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
            getErrorReporter().reportError(ioExc);
        }
    }

    public DeviceManager getDeviceManager()
    {
        return devMgr;
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

            // Initialize the Satellite module with the SYSTEM security context
            Initializer sysInit = new Initializer();
            Satellite instance = sysInit.initSatellite(cArgs, errorLog);

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

    public StltApiCallHandler getApiCallHandler()
    {
        return apiCallHandler;
    }

    public NodeData getLocalNode()
    {
        return controllerPeerConnector.getLocalNode();
    }

    public Peer getControllerPeer()
    {
        return controllerPeerConnector.getControllerPeer();
    }

    public void setControllerPeer(
        Peer controllerPeerRef,
        UUID nodeUuid,
        String nodeName,
        UUID disklessStorPoolDfnUuid,
        UUID disklessStorPoolUuid
    )
    {
        controllerPeerConnector.setControllerPeer(
            controllerPeerRef,
            nodeUuid,
            nodeName,
            disklessStorPoolDfnUuid,
            disklessStorPoolUuid
        );
    }

    public void setControllerPeerToCurrentLocalNode()
    {
        controllerPeerConnector.setControllerPeerToCurrentLocalNode();
    }

    public long getCurrentFullSyncId()
    {
        return updateMonitor.getCurrentFullSyncId();
    }

    public long getCurrentAwaitedUpdateId()
    {
        return updateMonitor.getCurrentAwaitedUpdateId();
    }

    public void awaitedUpdateApplied()
    {
        updateMonitor.awaitedUpdateApplied();
    }

    public long getNextFullSyncId()
    {
        return updateMonitor.getNextFullSyncId();
    }

    public void setFullSyncApplied()
    {
        updateMonitor.setFullSyncApplied();
    }

    public boolean isCurrentFullSyncApplied()
    {
        return updateMonitor.isCurrentFullSyncApplied();
    }
}
