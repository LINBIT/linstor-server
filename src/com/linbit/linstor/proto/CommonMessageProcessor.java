package com.linbit.linstor.proto;

import com.google.inject.Key;
import com.google.inject.name.Names;
import com.linbit.WorkQueue;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.protobuf.ApiCallDescriptor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.MessageProcessor;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.transaction.TransactionMgr;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.MessageTypes;
import com.linbit.linstor.security.AccessContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.event.Level;

/**
 * Dispatcher for received messages
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CommonMessageProcessor implements MessageProcessor
{
    private final ReadWriteLock apiLock;

    private final ErrorReporter errorLog;
    private final WorkQueue workQ;
    private final LinStorScope apiCallScope;
    private final Provider<TransactionMgr> trnActProvider;
    private final Map<String, ApiEntry> apiCallMap;

    @Inject
    public CommonMessageProcessor(
        ErrorReporter errorLogRef,
        @Named(LinStorModule.MAIN_WORKER_POOL_NAME) WorkQueue workQRef,
        LinStorScope apiCallScopeRef,
        Map<String, Provider<ApiCall>> apiCallProviders,
        Map<String, ApiCallDescriptor> apiCallDescriptors,
        @Named(LinStorModule.TRANS_MGR_GENERATOR) Provider<TransactionMgr> trnActProviderRef
    )
    {
        ErrorCheck.ctorNotNull(CommonMessageProcessor.class, WorkQueue.class, workQRef);
        apiLock         = new ReentrantReadWriteLock();
        errorLog        = errorLogRef;
        workQ           = workQRef;
        apiCallScope    = apiCallScopeRef;
        trnActProvider  = trnActProviderRef;
        apiCallMap      = new TreeMap<>();

        Iterator<Map.Entry<String, Provider<ApiCall>>> providerIter = apiCallProviders.entrySet().iterator();
        while (providerIter.hasNext())
        {
            Map.Entry<String, Provider<ApiCall>> providerEntry = providerIter.next();
            String apiName = providerEntry.getKey();
            Provider<ApiCall> apiProv = providerEntry.getValue();
            ApiCallDescriptor apiDscr = apiCallDescriptors.get(apiName);
            if (apiDscr != null)
            {
                apiCallMap.put(
                    apiName,
                    new ApiEntry(
                        apiProv, apiDscr,
                        // TODO: Prepared for future feature: Mandatory authentication
                        //       Load value from ApiCall object
                        false
                    )
                );
            }
            else
            {
                errorLog.reportError(
                    Level.ERROR,
                    new ImplementationError(
                        ApiCallDescriptor.class.getSimpleName() + " entry is missing for API call object '" +
                        apiName + "'",
                        null
                    )
                );
            }
        }
    }

    public Map<String, ApiCallDescriptor> getApiCallDescriptors()
    {
        Map<String, ApiCallDescriptor> objMap;

        Lock readLock = apiLock.readLock();
        try
        {
            readLock.lock();
            objMap = new TreeMap<>();
            Iterator<Map.Entry<String, ApiEntry>> apiIter = apiCallMap.entrySet().iterator();
            while (apiIter.hasNext())
            {
                Map.Entry<String, ApiEntry> entry = apiIter.next();
                objMap.put(entry.getKey(), entry.getValue().descriptor);
            }
        }
        finally
        {
            readLock.unlock();
        }
        return objMap;
    }

    @Override
    public void processMessage(final Message msg, final TcpConnector connector, final Peer client)
    {
        workQ.submit(new MessageProcessorInvocation(this, msg, connector, client));
    }

    private void processMessageImpl(Message msg, TcpConnector connector, Peer client)
    {
        try
        {
            int msgType = msg.getType();
            switch (msgType)
            {
                case MessageTypes.DATA:
                    handleDataMessage(msg, connector, client);
                    break;
                case MessageTypes.PING:
                    client.sendPong();
                    break;
                case MessageTypes.PONG:
                    client.pongReceived();
                    break;
                default:
                    String peerAddress = null;
                    int port = 0;
                    InetSocketAddress peerSocketAddr = client.peerAddress();
                    if (peerSocketAddr != null)
                    {
                        peerAddress = peerSocketAddr.getAddress().toString();
                        port = peerSocketAddr.getPort();
                    }
                    // Reached when a message with an unknown message type is received
                    if (peerAddress != null)
                    {
                        errorLog.logDebug(
                            "Message of unknown type %d received on connector %s " +
                            "from peer at endpoint %s:%d",
                            msgType, client.getConnectorInstanceName(), peerAddress, port
                        );
                    }
                    else
                    {
                        errorLog.logDebug(
                            "Message of unknown type %d received on connector %s " +
                            "from peer at unknown endpoint address",
                            msgType, client.getConnectorInstanceName()
                        );
                    }
                    break;
            }
        }
        catch (IllegalMessageStateException msgExc)
        {
            errorLog.reportError(
                Level.DEBUG,
                msgExc,
                client.getAccessContext(),
                client,
                null
            );
        }
    }

    private void handleDataMessage(
        final Message msg,
        final TcpConnector connector,
        final Peer client
    )
        throws IllegalMessageStateException
    {
        try
        {
            byte[] msgData = msg.getData();
            ByteArrayInputStream msgDataIn = new ByteArrayInputStream(msgData);

            MsgHeaderOuterClass.MsgHeader header = MsgHeaderOuterClass.MsgHeader.parseDelimitedFrom(msgDataIn);
            if (header != null)
            {
                int msgId = header.getMsgId();
                String apiCallName = header.getApiCall();

                Lock readLock = apiLock.readLock();
                ApiEntry apiMapEntry;
                try
                {
                    readLock.lock();
                    apiMapEntry = apiCallMap.get(apiCallName);
                }
                finally
                {
                    readLock.unlock();
                }
                if (apiMapEntry != null)
                {
                    AccessContext clientAccCtx = client.getAccessContext();
                    TransactionMgr transMgr = trnActProvider.get();
                    apiCallScope.enter();
                    try
                    {
                        apiCallScope.seed(Key.get(AccessContext.class, PeerContext.class), clientAccCtx);
                        apiCallScope.seed(Peer.class, client);
                        apiCallScope.seed(Message.class, msg);
                        apiCallScope.seed(Key.get(Integer.class, Names.named(ApiModule.MSG_ID)), msgId);
                        apiCallScope.seed(TransactionMgr.class, transMgr);
                        ApiCall apiObj = apiMapEntry.provider.get();
                        apiObj.execute(msgDataIn);
                    }
                    catch (Exception | ImplementationError exc)
                    {
                        errorLog.reportError(
                            Level.ERROR,
                            exc,
                            client.getAccessContext(),
                            client,
                            "Execution of the '" + apiCallName + "' API failed due to an unhandled exception"
                        );
                    }
                    finally
                    {
                        if (transMgr != null)
                        {
                            transMgr.returnConnection();
                        }
                        apiCallScope.exit();
                    }
                }
                else
                {
                    // Send error reports to clients only (not to Satellites)
                    if (client.getNode() == null)
                    {
                        errorLog.reportError(
                            Level.TRACE,
                            new LinStorException(
                                "Non-existent API '" + apiCallName + "' called by the client",
                                "The API call '" + apiCallName + "' cannot be executed.",
                                "The specified API does not exist",
                                "- Correct the client application to call a supported API\n" +
                                    "- Load the API module required by the client application into the server\n",
                                    "The API call name specified by the client was:\n" +
                                        apiCallName
                                ),
                            client.getAccessContext(),
                            client,
                            "The request was received on connector service '" + connector.getInstanceName() + "' " +
                                "of type '" + connector.getServiceName() + "'"
                        );
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        MsgHeaderOuterClass.MsgHeader.newBuilder()
                            .setApiCall(ApiConsts.API_REPLY)
                            .setMsgId(msgId)
                            .build()
                            .writeDelimitedTo(baos);

                        MsgApiCallResponseOuterClass.MsgApiCallResponse.newBuilder()
                            .setMessageFormat("The requested function call cannot be executed.")
                            .setCauseFormat(
                                "Common causes of this error are:\n" +
                                "   - The function call name specified by the caller\n" +
                                "     (client side) is incorrect\n" +
                                "   - The requested function call was not loaded into\n" +
                                "     the system (server side)"
                            )
                            .setDetailsFormat("The requested function call name was '" + apiCallName + "'.")
                            .setRetCode(ApiConsts.UNKNOWN_API_CALL)
                            .build()
                            .writeDelimitedTo(baos);

                        client.sendMessage(baos.toByteArray());
                    }
                    else
                    {
                        errorLog.logDebug(
                            "Non-existent API '" + apiCallName + "' called by controller or satellite " + client.getId()
                        );
                    }
                }
            }
        }
        catch (IOException ioExc)
        {
            // TODO: - Send an error report to the peer that sent the faulty message
            //       - Probably only from the controller to a satellite or client,
            //         and from the satellite to a client; otherwise, this can create
            //         a loop if error reports are created due to receiving a
            //         malformed error report
            errorLog.reportError(
                Level.DEBUG,
                ioExc,
                client.getAccessContext(),
                client,
                null
            );
        }
    }

    private static class ApiEntry
    {
        final Provider<ApiCall> provider;
        final ApiCallDescriptor descriptor;
        final boolean           reqAuth;

        ApiEntry(
            final Provider<ApiCall> providerRef,
            final ApiCallDescriptor descriptorRef,
            final boolean           reqAuthFlag
        )
        {
            provider    = providerRef;
            descriptor  = descriptorRef;
            reqAuth     = reqAuthFlag;
        }
    }

    private static class MessageProcessorInvocation implements Runnable
    {
        private final CommonMessageProcessor proc;

        private final Message       msg;
        private final TcpConnector  connector;
        private final Peer          client;

        MessageProcessorInvocation(
            final CommonMessageProcessor procRef,
            final Message msgRef,
            final TcpConnector connectorRef,
            final Peer clientRef
        )
        {
            proc        = procRef;
            msg         = msgRef;
            connector   = connectorRef;
            client      = clientRef;
        }

        @Override
        public void run()
        {
            proc.processMessageImpl(msg, connector, client);
        }
    }
}
