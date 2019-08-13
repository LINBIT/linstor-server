package com.linbit.linstor.core;

import javax.inject.Inject;
import com.linbit.ImplementationError;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.PublicContext;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.MessageProcessor;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.netcom.TcpConnectorService;
import com.linbit.linstor.netcom.ssl.SslTcpConnectorService;
import com.linbit.linstor.proto.CommonMessageProcessor;
import com.linbit.linstor.security.AccessContext;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.Properties;

public final class SatelliteNetComInitializer
{
    public static final String NET_COM_DEFAULT_TYPE = "plain";
    public static final String NET_COM_DEFAULT_ADDR = "::0";
    public static final int NET_COM_DEFAULT_PORT = 3366;

    // TCP Service configuration file
    private static final String NET_COM_CONF_FILE = "satellite_netcom.cfg";
    // Plain TCP Service configuration keys
    private static final String NET_COM_CONF_TYPE_KEY = "type";
    private static final String NET_COM_CONF_BIND_ADDR_KEY = "bind-address";
    private static final String NET_COM_CONF_PORT_KEY = "port";
    private static final String NET_COM_CONF_TYPE_PLAIN = "plain";
    private static final String NET_COM_CONF_TYPE_SSL = "ssl";
    // SSL Service configuration keys
    private static final String NET_COM_CONF_SSL_SERVER_CERT_KEY = "server-certificate";
    private static final String NET_COM_CONF_SSL_TRUST_CERT_KEY = "trusted-certificates";
    private static final String NET_COM_CONF_SSL_KEY_PASS_KEY = "key-passwd";
    private static final String NET_COM_CONF_SSL_KEYSTORE_PASS_KEY = "keystore-passwd";
    private static final String NET_COM_CONF_SSL_TRUST_PASS_KEY = "truststore-passwd";
    private static final String NET_COM_CONF_SSL_PROTOCOL_KEY = "ssl-protocol";

    private final ErrorReporter errorReporter;
    private final CommonSerializer commonSerializer;
    private final AccessContext publicCtx;
    private final MessageProcessor msgProc;
    private final StltConnTracker stltConnTracker;
    private final Map<ServiceName, SystemService> systemServicesMap;
    private final SatelliteConfigToml stltConfig;

    @Inject
    public SatelliteNetComInitializer(
        ErrorReporter errorReporterRef,
        CommonSerializer commonSerializerRef,
        @PublicContext AccessContext publicCtxRef,
        CommonMessageProcessor msgProcRef,
        StltConnTracker stltConnTrackerRef,
        Map<ServiceName, SystemService> systemServicesMapRef,
        SatelliteConfigToml stltConfigRef
    )
    {
        errorReporter = errorReporterRef;
        commonSerializer = commonSerializerRef;
        publicCtx = publicCtxRef;
        msgProc = msgProcRef;
        stltConnTracker = stltConnTrackerRef;
        systemServicesMap = systemServicesMapRef;
        stltConfig = stltConfigRef;
    }

    public boolean initMainNetComService(
        final AccessContext initCtx,
        final Path configurationDirectory,
        final String ovrdBindAddressStr,
        final Integer ovrdPlainPort
    )
    {
        boolean success = false;
        try
        {
            Properties netComProps = new Properties();
            Path stltNetComConfFile = configurationDirectory.resolve(NET_COM_CONF_FILE);
            if (Files.exists(stltNetComConfFile))
            {
                try (InputStream propsIn = new FileInputStream(stltNetComConfFile.toFile()))
                {
                    netComProps.loadFromXML(propsIn);
                }
                catch (FileNotFoundException fileExc)
                {
                    // this should never happen due to the if (Files.exists(...)), but if it
                    // still happens it can be ignored, as every property has a default-value
                }
            }

            String bindAddressStr = ovrdBindAddressStr;
            if (bindAddressStr == null)
            {
                bindAddressStr = netComProps.getProperty(
                    NET_COM_CONF_BIND_ADDR_KEY,
                    stltConfig.getNETCOM().getBindAddress()
                );
            }
            InetAddress addr = InetAddress.getByName(bindAddressStr);
            String portProp = netComProps.getProperty(
                NET_COM_CONF_PORT_KEY,
                stltConfig.getNETCOM().getPort().toString()
            );
            int port = Integer.parseInt(portProp);
            SocketAddress bindAddress = new InetSocketAddress(addr, port);

            TcpConnector netComSvc = null;

            String type = netComProps.getProperty(NET_COM_CONF_TYPE_KEY, stltConfig.getNETCOM().getType());
            if (type.equalsIgnoreCase(NET_COM_CONF_TYPE_PLAIN))
            {
                if (ovrdPlainPort != null)
                {
                    bindAddress = new InetSocketAddress(addr, ovrdPlainPort);
                    port = ovrdPlainPort;
                }
                netComSvc = new TcpConnectorService(
                    errorReporter,
                    commonSerializer,
                    msgProc,
                    bindAddress,
                    publicCtx,
                    initCtx,
                    stltConnTracker
                );
            }
            else
            if (type.equalsIgnoreCase(NET_COM_CONF_TYPE_SSL))
            {
                String sslProtocol = netComProps.getProperty(
                    NET_COM_CONF_SSL_PROTOCOL_KEY,
                    stltConfig.getNETCOM().getSslProtocol()
                );
                String keyStoreFile = netComProps.getProperty(
                    NET_COM_CONF_SSL_SERVER_CERT_KEY,
                    stltConfig.getNETCOM().getServerCertificate()
                );
                String trustStoreFile = netComProps.getProperty(
                    NET_COM_CONF_SSL_TRUST_CERT_KEY,
                    stltConfig.getNETCOM().getTrustedCertificates()
                );
                char[] keyPasswd = netComProps.getProperty(
                    NET_COM_CONF_SSL_KEY_PASS_KEY,
                    stltConfig.getNETCOM().getKeyPassword()
                ).toCharArray();
                char[] keyStorePasswd = netComProps.getProperty(
                    NET_COM_CONF_SSL_KEYSTORE_PASS_KEY,
                    stltConfig.getNETCOM().getKeystorePassword()
                ).toCharArray();
                char[] trustStorePasswd = netComProps.getProperty(
                    NET_COM_CONF_SSL_TRUST_PASS_KEY,
                    stltConfig.getNETCOM().getTrustStorePassword()
                ).toCharArray();

                try
                {
                    netComSvc = new SslTcpConnectorService(
                        errorReporter,
                        commonSerializer,
                        msgProc,
                        bindAddress,
                        publicCtx,
                        initCtx,
                        stltConnTracker,
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
                    systemServicesMap.put(netComSvc.getInstanceName(), netComSvc);
                    netComSvc.start();
                    errorReporter.logInfo(
                        String.format(
                            "%s started on port %s:%d",
                            netComSvc.getInstanceName().displayValue,
                            addr, port
                        )
                    );
                    success = true;
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
        return success;
    }
}
