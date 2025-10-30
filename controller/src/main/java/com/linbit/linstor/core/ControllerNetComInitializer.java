package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.SystemServiceStopException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PublicContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;
import com.linbit.linstor.netcom.ConnectionObserver;
import com.linbit.linstor.netcom.MessageProcessor;
import com.linbit.linstor.netcom.NetComContainer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.netcom.TcpConnectorService;
import com.linbit.linstor.netcom.ssl.SslTcpConnectorService;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.proto.CommonMessageProcessor;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.systemstarter.StartupInitializer;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import javax.inject.Inject;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.event.Level;

public final class ControllerNetComInitializer implements StartupInitializer
{
    private static final String PROPSCON_KEY_NETCOM = ApiConsts.NAMESPC_NETCOM;
    private static final String PROPSCON_KEY_NETCOM_BINDADDR = ApiConsts.KEY_NETCOM_BIND_ADDRESS;
    private static final String PROPSCON_KEY_NETCOM_PORT = ApiConsts.KEY_NETCOM_PORT;
    private static final String PROPSCON_KEY_NETCOM_TYPE = ApiConsts.KEY_NETCOM_TYPE;
    private static final String PROPSCON_KEY_NETCOM_TRUSTSTORE = ApiConsts.KEY_NETCOM_TRUST_STORE;
    private static final String PROPSCON_KEY_NETCOM_TRUSTSTORE_PASSWD = ApiConsts.KEY_NETCOM_TRUST_STORE_PASSWD;
    private static final String PROPSCON_KEY_NETCOM_KEYSTORE = ApiConsts.KEY_NETCOM_KEY_STORE;
    private static final String PROPSCON_KEY_NETCOM_KEYSTORE_PASSWD = ApiConsts.KEY_NETCOM_KEY_STORE_PASSWD;
    private static final String PROPSCON_KEY_NETCOM_KEY_PASSWD = ApiConsts.KEY_NETCOM_KEY_PASSWD;
    private static final String PROPSCON_KEY_NETCOM_SSL_PROTOCOL = ApiConsts.KEY_NETCOM_SSL_PROTOCOL;
    private static final String PROPSCON_KEY_NETCOM_ENABLED = ApiConsts.KEY_NETCOM_ENABLED;
    private static final String PROPSCON_NETCOM_TYPE_PLAIN = ApiConsts.VAL_NETCOM_TYPE_PLAIN;
    private static final String PROPSCON_NETCOM_TYPE_SSL = ApiConsts.VAL_NETCOM_TYPE_SSL;
    static final String PROPSCON_KEY_DEFAULT_DEBUG_SSL_CON_SVC = "defaultDebugSslConnector";
    static final String PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC = "defaultPlainConSvc";
    static final String PROPSCON_KEY_DEFAULT_SSL_CON_SVC = "defaultSslConSvc";

    private final ErrorReporter errorReporter;
    private final CommonSerializer commonSerializer;
    private final AccessContext sysCtx;
    private final AccessContext publicCtx;
    private final Props ctrlConf;
    private final MessageProcessor msgProc;
    private final ConnectionObserver ctrlConnTracker;
    private final ModularCryptoProvider cryptoProvider;
    private final NetComContainer netComContainer;
    private final Map<ServiceName, SystemService> systemServicesMap;
    private final LinStorScope initScope;
    private final TransactionMgrGenerator transactionMgrGenerator;
    private final CtrlConfig ctrlCfg;

    private @Nullable TcpConnector netComSvc;

    @Inject
    public ControllerNetComInitializer(
        ErrorReporter errorReporterRef,
        CommonSerializer commonSerializerRef,
        @SystemContext AccessContext sysCtxRef,
        @PublicContext AccessContext publicCtxRef,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef,
        CommonMessageProcessor msgProcRef,
        CtrlConnTracker ctrlConnTrackerRef,
        ModularCryptoProvider cryptoProviderRef,
        NetComContainer netComContainerRef,
        Map<ServiceName, SystemService> systemServicesMapRef,
        LinStorScope initScopeRef,
        TransactionMgrGenerator transactionMgrGeneratorRef,
        CtrlConfig ctrlCfgRef
    )
    {
        errorReporter = errorReporterRef;
        commonSerializer = commonSerializerRef;
        sysCtx = sysCtxRef;
        publicCtx = publicCtxRef;
        ctrlConf = ctrlConfRef;
        msgProc = msgProcRef;
        ctrlConnTracker = ctrlConnTrackerRef;
        cryptoProvider = cryptoProviderRef;
        netComContainer = netComContainerRef;
        systemServicesMap = systemServicesMapRef;
        initScope = initScopeRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
        ctrlCfg = ctrlCfgRef;
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
        TcpConnector curNetComSvc = netComContainer.getNetComConnector(serviceName);
        SystemService sysSvc = systemServicesMap.get(serviceName);

        boolean svcStarted = false;
        boolean issuedShutdown = false;
        if (curNetComSvc != null)
        {
            svcStarted = curNetComSvc.isStarted();
            if (svcStarted)
            {
                curNetComSvc.shutdown(false);
                issuedShutdown = true;
            }
        }
        else
        if (sysSvc != null)
        {
            svcStarted = sysSvc.isStarted();
            if (svcStarted)
            {
                sysSvc.shutdown(false);
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

        if (curNetComSvc != null || sysSvc != null)
        {
            errorLogRef.logInfo(
                String.format(
                    "Deleted network communication service '%s'",
                    serviceName.displayValue
                )
            );
        }

        return curNetComSvc != null || sysSvc != null;
    }

    @Override
    public void initialize()
        throws SystemServiceStartException
    {
        errorReporter.logInfo("Initializing network communications services");
        @Nullable ReadOnlyProps netComProps = ctrlConf.getNamespace(PROPSCON_KEY_NETCOM);

        if (netComProps == null)
        {
            String errorMsg = "The controller configuration does not define any network communication services";
            throw new SystemServiceStartException(
                errorMsg,
                errorMsg,
                null,
                null,
                "Define at least one network communication service",
                null,
                false
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
                    errorReporter,
                    sysCtx
                );
            }
            catch (SystemServiceStartException sysSvcStartExc)
            {
                errorReporter.reportError(Level.ERROR, sysSvcStartExc);
            }
        }
    }

    @Override
    public void shutdown(boolean jvmShutdownRef)
    {
        if (netComSvc != null)
        {
            netComSvc.shutdown(jvmShutdownRef);
        }
    }

    @Override
    public void awaitShutdown(long timeout) throws InterruptedException
    {
        if (netComSvc != null)
        {
            netComSvc.awaitShutdown(timeout);
        }
    }

    private Path resolveConfigFilePath(final String filePath)
    {
        Path path = Paths.get(filePath);
        if (!path.isAbsolute())
        {
            path = ctrlCfg.getConfigPath().resolve(path);
        }

        return path;
    }

    private void createNetComService(
        ServiceName serviceName,
        ReadOnlyProps configProp,
        ErrorReporter errorLogRef,
        AccessContext initCtx
    )
        throws SystemServiceStartException
    {
        String bindAddressStr = loadPropChecked(configProp, PROPSCON_KEY_NETCOM_BINDADDR);
        int port = Integer.parseInt(loadPropChecked(configProp, PROPSCON_KEY_NETCOM_PORT));
        String type = loadPropChecked(configProp, PROPSCON_KEY_NETCOM_TYPE);

        final SocketAddress bindAddress =
            !bindAddressStr.isEmpty() ? new InetSocketAddress(bindAddressStr, port) : null;

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
                if (dfltPlainConSvc == null || dfltPlainConSvc.isEmpty())
                {
                    TransactionMgr transMgr = null;
                    try (LinStorScope.ScopeAutoCloseable close = initScope.enter())
                    {
                        transMgr = transactionMgrGenerator.startTransaction();
                        TransactionMgrUtil.seedTransactionMgr(initScope, transMgr);

                        ctrlConf.setProp(PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC, serviceName.displayValue);

                        transMgr.commit();
                    }
                    catch (DatabaseException dbExc)
                    {
                        errorLogRef.reportError(
                            dbExc,
                            sysCtx,
                            null,
                            "A database exception was thrown while trying to persist the default plain connector"
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
                            catch (TransactionException sqlExc2)
                            {
                                errorLogRef.reportError(
                                    sqlExc2,
                                    sysCtx,
                                    null,
                                    "A database exception was thrown while trying to rollback a transaction"
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
                        cryptoProvider,
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
                            (dfltSslSvcName == null || dfltSslSvcName.isEmpty()))
                        {
                            TransactionMgr transMgr = null;
                            try (LinStorScope.ScopeAutoCloseable close = initScope.enter())
                            {
                                transMgr = transactionMgrGenerator.startTransaction();
                                TransactionMgrUtil.seedTransactionMgr(initScope, transMgr);

                                ctrlConf.setProp(PROPSCON_KEY_DEFAULT_SSL_CON_SVC, serviceName.displayValue);

                                transMgr.commit();
                            }
                            catch (DatabaseException dbExc)
                            {
                                errorLogRef.reportError(
                                    dbExc,
                                    sysCtx,
                                    null,
                                    "A database exception was thrown while trying to persist the default ssl connector"
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
                                    catch (TransactionException sqlExc2)
                                    {
                                        errorLogRef.reportError(
                                            sqlExc2,
                                            sysCtx,
                                            null,
                                            "A database exception was thrown while trying to rollback a transaction"
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
                    exc,
                    false
                );
            }
        }
        else
        {
            errorLogRef.reportError(
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
                String.format("Created network communication service '%s'", serviceName.displayValue) +
                        (bindAddress != null ? String.format(", bound to %s:%d", bindAddressStr, port) : "")
            );
        }

    }

    private void initNetComService(
        String serviceNameStr,
        ReadOnlyProps netComProps,
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
                invalidNameExc,
                false
            );
        }
        @Nullable ReadOnlyProps configProp = netComProps.getNamespace(serviceNameStr);
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

    private String loadPropChecked(ReadOnlyProps props, String key) throws SystemServiceStartException
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
                    null,
                    false
                );
            }

        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ImplementationError("Constant key is invalid " + key, invalidKeyExc);
        }

        return value;
    }

    private @Nullable String loadOrAddKey(ReadOnlyProps props, String key, List<String> missingKeys)
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

    private String loadProp(ReadOnlyProps props, String key, String defaultValue)
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
