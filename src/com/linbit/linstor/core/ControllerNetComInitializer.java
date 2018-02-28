package com.linbit.linstor.core;

import com.google.inject.Inject;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.SystemServiceStopException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.PublicContext;
import com.linbit.linstor.annotation.SystemContext;
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
import org.slf4j.event.Level;

import javax.inject.Named;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.util.Iterator;
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
    private static final String PROPSCON_NETCOM_TYPE_PLAIN = "plain";
    private static final String PROPSCON_NETCOM_TYPE_SSL = "ssl";
    static final String PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC = "defaultPlainConSvc";
    static final String PROPSCON_KEY_DEFAULT_SSL_CON_SVC = "defaultSslConSvc";

    private final ErrorReporter errorReporter;
    private final AccessContext sysCtx;
    private final AccessContext publicCtx;
    private final Props ctrlConf;
    private final DbConnectionPool dbConnPool;
    private final MessageProcessor msgProc;
    private final ConnectionObserver ctrlConnTracker;
    private final NetComContainer netComContainer;
    private final Map<ServiceName, SystemService> systemServicesMap;

    @Inject
    public ControllerNetComInitializer(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext sysCtxRef,
        @PublicContext AccessContext publicCtxRef,
        @Named(ControllerCoreModule.CONTROLLER_PROPS) Props ctrlConfRef,
        DbConnectionPool dbConnPoolRef,
        CommonMessageProcessor msgProcRef,
        CtrlConnTracker ctrlConnTrackerRef,
        NetComContainer netComContainerRef,
        Map<ServiceName, SystemService> systemServicesMapRef
    )
    {
        errorReporter = errorReporterRef;
        sysCtx = sysCtxRef;
        publicCtx = publicCtxRef;
        ctrlConf = ctrlConfRef;
        dbConnPool = dbConnPoolRef;
        msgProc = msgProcRef;
        ctrlConnTracker = ctrlConnTrackerRef;
        netComContainer = netComContainerRef;
        systemServicesMap = systemServicesMapRef;
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
    {
        errorLogRef.logInfo("Initializing network communications services");

        if (netComProps == null)
        {
            String errorMsg = "The controller configuration does not define any network communication services";
            errorLogRef.reportError(
                new SystemServiceStartException(
                    errorMsg,
                    errorMsg,
                    null,
                    null,
                    "Define at least one network communication service",
                    null
                )
            );
        }
        else
        {
            Iterator<String> namespaces = netComProps.iterateNamespaces();
            while (namespaces.hasNext())
            {
                try
                {
                    String namespaceStr = namespaces.next();
                    createNetComService(
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
    }

    private void createNetComService(
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
        Props configProp;
        try
        {
            configProp = netComProps.getNamespace(serviceNameStr);
        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ImplementationError(
                String.format(
                    "A properties container returned the key '%s' as the identifier for a namespace, " +
                    "but using the same key to obtain a reference to the namespace generated an " +
                    "%s",
                    serviceName,
                    invalidKeyExc.getClass().getSimpleName()
                ),
                invalidKeyExc
            );
        }
        String bindAddressStr = loadPropChecked(configProp, PROPSCON_KEY_NETCOM_BINDADDR);
        Integer port = Integer.parseInt(loadPropChecked(configProp, PROPSCON_KEY_NETCOM_PORT));
        String type = loadPropChecked(configProp, PROPSCON_KEY_NETCOM_TYPE);

        SocketAddress bindAddress = new InetSocketAddress(bindAddressStr, port);

        TcpConnector netComSvc = null;
        if (type.equals(PROPSCON_NETCOM_TYPE_PLAIN))
        {
            netComSvc = new TcpConnectorService(
                errorReporter,
                msgProc,
                bindAddress,
                publicCtx,
                initCtx,
                ctrlConnTracker
            );
            try
            {
                if (ctrlConf.getProp(PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC) == null)
                {
                    TransactionMgr transMgr = null;
                    try
                    {
                        transMgr = new TransactionMgr(dbConnPool);
                        ctrlConf.setConnection(transMgr);
                        ctrlConf.setProp(PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC, serviceName.displayValue);
                        transMgr.commit();
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
                            dbConnPool.returnConnection(transMgr);
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
                netComSvc = new SslTcpConnectorService(
                    errorReporter,
                    msgProc,
                    bindAddress,
                    publicCtx,
                    initCtx,
                    ctrlConnTracker,
                    loadPropChecked(configProp, PROPSCON_KEY_NETCOM_SSL_PROTOCOL),
                    loadPropChecked(configProp, PROPSCON_KEY_NETCOM_KEYSTORE),
                    loadPropChecked(configProp, PROPSCON_KEY_NETCOM_KEYSTORE_PASSWD).toCharArray(),
                    loadPropChecked(configProp, PROPSCON_KEY_NETCOM_KEY_PASSWD).toCharArray(),
                    loadPropChecked(configProp, PROPSCON_KEY_NETCOM_TRUSTSTORE),
                    loadPropChecked(configProp, PROPSCON_KEY_NETCOM_TRUSTSTORE_PASSWD).toCharArray()
                );
                try
                {
                    if (ctrlConf.getProp(PROPSCON_KEY_DEFAULT_SSL_CON_SVC) == null)
                    {

                        TransactionMgr transMgr = null;
                        try
                        {
                            transMgr = new TransactionMgr(dbConnPool);
                            ctrlConf.setConnection(transMgr);
                            ctrlConf.setProp(PROPSCON_KEY_DEFAULT_SSL_CON_SVC, serviceName.displayValue);
                            transMgr.commit();
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
                                dbConnPool.returnConnection(transMgr);
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
            errorLogRef.logInfo(
                String.format(
                    "Created network communication service '%s', bound to %s:%d",
                    serviceName.displayValue, bindAddressStr, port
                )
            );
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
}
