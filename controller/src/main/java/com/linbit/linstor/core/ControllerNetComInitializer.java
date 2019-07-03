package com.linbit.linstor.core;

import javax.inject.Inject;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.SystemServiceStopException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.PublicContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.ConnectionObserver;
import com.linbit.linstor.netcom.MessageProcessor;
import com.linbit.linstor.netcom.NetComContainer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.netcom.TcpConnectorService;
import com.linbit.linstor.netcom.ssl.SslTcpConnectorService;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.CommonMessageProcessor;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.ControllerTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;

import org.slf4j.event.Level;

import javax.inject.Named;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class ControllerNetComInitializer
{
    private static final String PROPSCON_KEY_NETCOM_BINDADDR = "bindaddress";
    private static final String PROPSCON_KEY_NETCOM_PORT = "port";
    private static final String PROPSCON_KEY_NETCOM_TYPE = "type";
    private static final String PROPSCON_KEY_NETCOM_TRUSTSTORE = "trustStore";
    private static final String PROPSCON_KEY_NETCOM_TRUSTSTORE_PASSWD = "trustStorePasswd";
    private static final String PROPSCON_KEY_NETCOM_KEYSTORE = "keyStore";
    private static final String PROPSCON_KEY_NETCOM_KEYSTORE_PASSWD = "keyStorePasswd";
    private static final String PROPSCON_KEY_NETCOM_KEY_PASSWD = "keyPasswd";
    private static final String PROPSCON_KEY_NETCOM_SSL_PROTOCOL = "sslProtocol";
    private static final String PROPSCON_KEY_NETCOM_ENABLED = "enabled";
    private static final String PROPSCON_NETCOM_TYPE_PLAIN = "plain";
    private static final String PROPSCON_NETCOM_TYPE_SSL = "ssl";
    static final String PROPSCON_KEY_DEFAULT_DEBUG_SSL_CON_SVC = "defaultDebugSslConnector";
    static final String PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC = "defaultPlainConSvc";
    static final String PROPSCON_KEY_DEFAULT_SSL_CON_SVC = "defaultSslConSvc";

    private final ErrorReporter errorReporter;
    private final CommonSerializer commonSerializer;
    private final AccessContext sysCtx;
    private final AccessContext publicCtx;
    private final Props ctrlConf;
    private final DbConnectionPool dbConnPool;
    private final MessageProcessor msgProc;
    private final ConnectionObserver ctrlConnTracker;
    private final NetComContainer netComContainer;
    private final Map<ServiceName, SystemService> systemServicesMap;
    private final LinStorScope initScope;
    private final ControllerCmdlArguments controllerCmdlArguments;

    @Inject
    public ControllerNetComInitializer(
        ErrorReporter errorReporterRef,
        CommonSerializer commonSerializerRef,
        @SystemContext AccessContext sysCtxRef,
        @PublicContext AccessContext publicCtxRef,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef,
        DbConnectionPool dbConnPoolRef,
        CommonMessageProcessor msgProcRef,
        CtrlConnTracker ctrlConnTrackerRef,
        NetComContainer netComContainerRef,
        Map<ServiceName, SystemService> systemServicesMapRef,
        LinStorScope initScopeRef,
        ControllerCmdlArguments controllerCmdlArgumentsRef
    )
    {
        errorReporter = errorReporterRef;
        commonSerializer = commonSerializerRef;
        sysCtx = sysCtxRef;
        publicCtx = publicCtxRef;
        ctrlConf = ctrlConfRef;
        dbConnPool = dbConnPoolRef;
        msgProc = msgProcRef;
        ctrlConnTracker = ctrlConnTrackerRef;
        netComContainer = netComContainerRef;
        systemServicesMap = systemServicesMapRef;
        initScope = initScopeRef;
        controllerCmdlArguments = controllerCmdlArgumentsRef;
    }

    public boolean deleteNetComService(String serviceNameStr, ErrorReporter errorLogRef)
        throws SystemServiceStopException
    {
        ServiceName serviceName;
        try
        {
            serviceName = new ServiceName(serviceNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new SystemServiceStopException(
                String.format(
                    "The name '%s' can not be used for a network communication service instance",
                    serviceNameStr
                ),
                String.format(
                    "The name '%s' is not a valid name for a network communication service instance",
                    serviceNameStr
                ),
                null,
                "Change the name of the network communication service instance",
                null,
                invalidNameExc
            );
        }
        TcpConnector netComSvc = netComContainer.getNetComConnector(serviceName);
        SystemService sysSvc = systemServicesMap.get(serviceName);

        boolean svcStarted = false;
        boolean issuedShutdown = false;
        if (netComSvc != null)
        {
            svcStarted = netComSvc.isStarted();
            if (svcStarted)
            {
                netComSvc.shutdown();
                issuedShutdown = true;
            }
        }
        else
        if (sysSvc != null)
        {
            svcStarted = sysSvc.isStarted();
            if (svcStarted)
            {
                sysSvc.shutdown();
                issuedShutdown = true;
            }
        }

        netComContainer.removeNetComConnector(serviceName);
        systemServicesMap.remove(serviceName);

        if (svcStarted && issuedShutdown)
        {
            errorLogRef.logInfo(
                String.format(
                    "Initiated shutdown of network communication service '%s'",
                    serviceName.displayValue
                )
            );
        }

        if (netComSvc != null || sysSvc != null)
        {
            errorLogRef.logInfo(
                String.format(
                    "Deleted network communication service '%s'",
                    serviceName.displayValue
                )
            );
        }

        return netComSvc != null || sysSvc != null;
    }

    public void initNetComServices(
        Props netComProps,
        ErrorReporter errorLogRef,
        AccessContext initCtx
    )
        throws SystemServiceStartException
    {
        errorLogRef.logInfo("Initializing network communications services");

        if (netComProps == null)
        {
            String errorMsg = "The controller configuration does not define any network communication services";
            throw new SystemServiceStartException(
                errorMsg,
                errorMsg,
                null,
                null,
                "Define at least one network communication service",
                null
            );
        }

        Iterator<String> namespaces = netComProps.iterateNamespaces();
        while (namespaces.hasNext())
        {
            try
            {
                String namespaceStr = namespaces.next();
                initNetComService(
                    namespaceStr,
                    netComProps,
                    errorLogRef,
                    initCtx
                );
            }
            catch (SystemServiceStartException sysSvcStartExc)
            {
                errorLogRef.reportProblem(Level.ERROR, sysSvcStartExc, null, null, null);
            }
        }
    }

    private Path resolveConfigFilePath(final String filePath)
    {
        Path path = Paths.get(filePath);
        if (!path.isAbsolute())
        {
            path = Paths.get(controllerCmdlArguments.getConfigurationDirectory())
                .resolve(path);
        }

        return path;
    }

    private void createNetComService(
        ServiceName serviceName,
        Props configProp,
        ErrorReporter errorLogRef,
        AccessContext initCtx
    )
        throws SystemServiceStartException
    {
        String bindAddressStr = loadPropChecked(configProp, PROPSCON_KEY_NETCOM_BINDADDR);
        Integer port = Integer.parseInt(loadPropChecked(configProp, PROPSCON_KEY_NETCOM_PORT));
        String type = loadPropChecked(configProp, PROPSCON_KEY_NETCOM_TYPE);

        SocketAddress bindAddress = new InetSocketAddress(bindAddressStr, port);

        TcpConnector netComSvc = null;
        if (type.equals(PROPSCON_NETCOM_TYPE_PLAIN))
        {
            netComSvc = new TcpConnectorService(
                errorReporter,
                commonSerializer,
                msgProc,
                bindAddress,
                publicCtx,
                initCtx,
                ctrlConnTracker
            );
            try
            {
                String dfltPlainConSvc = ctrlConf.getProp(PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC);
                if (dfltPlainConSvc == null || dfltPlainConSvc.equals(""))
                {
                    TransactionMgr transMgr = null;
                    try
                    {
                        transMgr = new ControllerTransactionMgr(dbConnPool);
                        initScope.enter();
                        initScope.seed(TransactionMgr.class, transMgr);

                        ctrlConf.setProp(PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC, serviceName.displayValue);

                        transMgr.commit();
                        initScope.exit();
                    }
                    catch (SQLException sqlExc)
                    {
                        errorLogRef.reportError(
                            sqlExc,
                            sysCtx,
                            null,
                            "An SQL exception was thrown while trying to persist the default plain connector"
                        );
                    }
                    finally
                    {
                        if (transMgr != null)
                        {
                            try
                            {
                                transMgr.rollback();
                            }
                            catch (SQLException sqlExc2)
                            {
                                errorLogRef.reportError(
                                    sqlExc2,
                                    sysCtx,
                                    null,
                                    "An SQL exception was thrown while trying to rollback a transaction"
                                );
                            }
                            transMgr.returnConnection();
                        }
                    }
                }
            }
            catch (AccessDeniedException | InvalidKeyException | InvalidValueException exc)
            {
                errorLogRef.reportError(
                    new ImplementationError(
                        "Storing default plain connector service caused exception",
                        exc
                    )
                );
            }
        }
        else
        if (type.equals(PROPSCON_NETCOM_TYPE_SSL))
        {
            try
            {
                List<String> missingPropKeys = new ArrayList<>();
                String sslProtocol = loadOrAddKey(configProp, PROPSCON_KEY_NETCOM_SSL_PROTOCOL, missingPropKeys);
                String keyStoreFile = loadOrAddKey(configProp, PROPSCON_KEY_NETCOM_KEYSTORE, missingPropKeys);
                String keyStorePw = loadOrAddKey(configProp, PROPSCON_KEY_NETCOM_KEYSTORE_PASSWD, missingPropKeys);
                String keyPw = loadOrAddKey(configProp, PROPSCON_KEY_NETCOM_KEY_PASSWD, missingPropKeys);
                String trustStoreFile = loadOrAddKey(configProp, PROPSCON_KEY_NETCOM_TRUSTSTORE, missingPropKeys);
                String trustStorPw = loadOrAddKey(configProp, PROPSCON_KEY_NETCOM_TRUSTSTORE_PASSWD, missingPropKeys);

                // resolve SSL file paths
                Path keyStoreFilePath = resolveConfigFilePath(keyStoreFile);
                Path trustStoreFilePath = resolveConfigFilePath(trustStoreFile);

                boolean rejectStart = false;
                if (!missingPropKeys.isEmpty())
                {
                    StringBuilder errorMsg = new StringBuilder();
                    errorMsg.append("The SSL TCP connector '").append(serviceName.displayValue)
                        .append("' could not be started as it is missing the following key");
                    if (missingPropKeys.size() > 1)
                    {
                        errorMsg.append("s");
                    }
                    errorMsg.append(":");
                    for (String missingKey : missingPropKeys)
                    {
                        errorMsg.append("\\n\t").append(missingKey);
                    }
                    errorLogRef.logWarning(errorMsg.toString());
                    rejectStart = true;
                }
                else
                {
                    boolean keyStorFileExists = Files.exists(keyStoreFilePath);
                    boolean trustStoreFileExists = Files.exists(trustStoreFilePath);

                    if (!keyStorFileExists)
                    {
                        String errorMsg = String.format("The SSL network communication service '%s' " +
                            "could not be started because the keyStore file (%s) is missing",
                            serviceName.displayValue,
                            keyStoreFilePath.toString()
                        );
                        errorLogRef.logWarning(errorMsg);
                        rejectStart = true;
                    }
                    else
                    if (!trustStoreFileExists)
                    {
                        String errorMsg = String.format("The SSL network communication service '%s' " +
                            "is missing the trustStor file (%s)",
                            serviceName.displayValue,
                            trustStoreFilePath.toString()
                        );
                        errorLogRef.logWarning(errorMsg);
                        rejectStart = false;
                    }
                }

                if (!rejectStart)
                {
                    netComSvc = new SslTcpConnectorService(
                        errorReporter,
                        commonSerializer,
                        msgProc,
                        bindAddress,
                        publicCtx,
                        initCtx,
                        ctrlConnTracker,
                        sslProtocol,
                        keyStoreFilePath.toString(),
                        keyStorePw.toCharArray(),
                        keyPw.toCharArray(),
                        trustStoreFilePath.toString(),
                        trustStorPw.toCharArray()
                    );
                    try
                    {
                        String dfltDebugSslSvcName = ctrlConf.getProp(PROPSCON_KEY_DEFAULT_DEBUG_SSL_CON_SVC);
                        String dfltSslSvcName = ctrlConf.getProp(PROPSCON_KEY_DEFAULT_SSL_CON_SVC);

                        if (!serviceName.value.equals(dfltDebugSslSvcName.toUpperCase()) &&
                            (dfltSslSvcName == null || dfltSslSvcName.equals("")))
                        {
                            TransactionMgr transMgr = null;
                            try
                            {
                                transMgr = new ControllerTransactionMgr(dbConnPool);
                                initScope.enter();
                                initScope.seed(TransactionMgr.class, transMgr);

                                ctrlConf.setProp(PROPSCON_KEY_DEFAULT_SSL_CON_SVC, serviceName.displayValue);

                                transMgr.commit();
                                initScope.exit();
                            }
                            catch (SQLException sqlExc)
                            {
                                errorLogRef.reportError(
                                    sqlExc,
                                    sysCtx,
                                    null,
                                    "An SQL exception was thrown while trying to persist the default ssl connector"
                                );
                            }
                            finally
                            {
                                if (transMgr != null)
                                {
                                    try
                                    {
                                        transMgr.rollback();
                                    }
                                    catch (SQLException sqlExc2)
                                    {
                                        errorLogRef.reportError(
                                            sqlExc2,
                                            sysCtx,
                                            null,
                                            "An SQL exception was thrown while trying to rollback a transaction"
                                        );
                                    }
                                    transMgr.returnConnection();
                                }
                            }
                        }
                    }
                    catch (AccessDeniedException | InvalidKeyException | InvalidValueException exc)
                    {
                        errorLogRef.reportError(
                            new ImplementationError(
                                "Storing default ssl connector service caused exception",
                                exc
                            )
                        );
                    }
                }
            }
            catch (
                KeyManagementException | UnrecoverableKeyException |
                    NoSuchAlgorithmException | KeyStoreException | CertificateException |
                    IOException exc
                )
            {
                String errorMsg = "Initialization of an SSL-enabled network communication service failed";
                errorLogRef.reportError(exc);
                throw new SystemServiceStartException(
                    errorMsg,
                    errorMsg,
                    null,
                    null,
                    null,
                    exc
                );
            }
        }
        else
        {
            errorLogRef.reportProblem(
                Level.ERROR,
                new LinStorException(
                    String.format(
                        "The connection type for the network communication service '%s' is not valid",
                        serviceName
                    ),
                    String.format(
                        "The connection type has to be either '%s' or '%s', but was '%s'",
                        PROPSCON_NETCOM_TYPE_PLAIN,
                        PROPSCON_NETCOM_TYPE_SSL,
                        type),
                    null,
                    "Correct the entry in the database",
                    null
                ),
                null, // accCtx
                null, // client
                null  // contextInfo
            );
        }

        if (netComSvc != null)
        {
            netComSvc.setServiceInstanceName(serviceName);
            netComContainer.putNetComContainer(serviceName, netComSvc);
            systemServicesMap.put(serviceName, netComSvc);
            netComSvc.start();
            errorLogRef.logInfo(
                String.format(
                    "Created network communication service '%s', bound to %s:%d",
                    serviceName.displayValue, bindAddressStr, port
                )
            );
        }

    }

    private void initNetComService(
        String serviceNameStr,
        Props netComProps,
        ErrorReporter errorLogRef,
        AccessContext initCtx
    )
        throws SystemServiceStartException
    {
        ServiceName serviceName;
        try
        {
            serviceName = new ServiceName(serviceNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new SystemServiceStartException(
                String.format(
                    "The name '%s' can not be used for a network communication service instance",
                    serviceNameStr
                ),
                String.format(
                    "The name '%s' is not a valid name for a network communication service instance",
                    serviceNameStr
                ),
                null,
                "Change the name of the network communication service instance",
                null,
                invalidNameExc
            );
        }
        Props configProp = netComProps.getNamespace(serviceNameStr).orElse(null);
        if (configProp == null)
        {
            errorLogRef.logError(
                "A properties container returned the key '%s' as the identifier for a namespace, " +
                    "but using the same key to obtain a reference to the namespace generated an " +
                    "%s",
                serviceName
            );
            throw new RuntimeException();
        }

        if (loadProp(configProp, PROPSCON_KEY_NETCOM_ENABLED, "true").equals("true"))
        {
            createNetComService(serviceName, configProp, errorLogRef, initCtx);
        }
        else
        {
            errorLogRef.logInfo(serviceName + " is not enabled.");
        }
    }

    private String loadPropChecked(Props props, String key) throws SystemServiceStartException
    {
        String value;
        try
        {
            value = props.getProp(key);
            if (value == null)
            {
                String errorMsg = String.format(
                    "The configuration entry '%s%s' is missing in the configuration",
                    props.getPath(), key
                );
                throw new SystemServiceStartException(
                    errorMsg,
                    errorMsg,
                    null,
                    "Add the missing configuration entry to the configuration",
                    null
                );
            }

        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ImplementationError("Constant key is invalid " + key, invalidKeyExc);
        }

        return value;
    }

    private String loadOrAddKey(Props props, String key, List<String> missingKeys)
    {
        String value = null;
        try
        {
            value = props.getProp(key);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Loading a hardcoded but invalid netcom-property key", exc);
        }
        if (value == null)
        {
            missingKeys.add(key);
        }
        return value;
    }

    private String loadProp(Props props, String key, String defaultValue)
    {
        String value;
        try
        {
            value = props.getPropWithDefault(key, defaultValue);
        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ImplementationError("Constant key is invalid " + key, invalidKeyExc);
        }

        return value;
    }
}
