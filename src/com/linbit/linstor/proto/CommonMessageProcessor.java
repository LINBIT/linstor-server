package com.linbit.linstor.proto;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.WorkQueue;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.protobuf.ApiCallDescriptor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.MessageProcessor;
import com.linbit.linstor.netcom.MessageTypes;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass.MsgApiCallResponse;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.TransactionMgr;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.slf4j.event.Level;

import com.google.inject.Key;
import com.google.inject.name.Names;

/**
 * Dispatcher for received messages
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class CommonMessageProcessor implements MessageProcessor
{
    private final ReadWriteLock apiLock;

    private final ErrorReporter errorLog;
    private final WorkQueue workQ;
    private final LinStorScope apiCallScope;
    private final Map<String, Provider<ApiCall>> apiCallProviders;
    private final Map<String, ApiCallDescriptor> apiCallDescriptors;
    private final Provider<TransactionMgr> transactionProvider;

    private final ControllerDatabase dbConnPool;

    @Inject
    public CommonMessageProcessor(
        ErrorReporter errorLogRef,
        @Named(LinStorModule.MAIN_WORKER_POOL_NAME) WorkQueue workQRef,
        LinStorScope apiCallScopeRef,
        Map<String, Provider<ApiCall>> apiCallProvidersRef,
        Map<String, ApiCallDescriptor> apiCallDescriptorsRef,
        @Named(LinStorModule.TRANS_MGR_GENERATOR) Provider<TransactionMgr> transactionProviderRef,
        ControllerDatabase dbConnPoolRef
    )
    {
        ErrorCheck.ctorNotNull(CommonMessageProcessor.class, WorkQueue.class, workQRef);
        apiLock     = new ReentrantReadWriteLock();
        errorLog    = errorLogRef;
        workQ       = workQRef;
        apiCallScope = apiCallScopeRef;
        apiCallProviders = apiCallProvidersRef;
        apiCallDescriptors = apiCallDescriptorsRef;
        transactionProvider = transactionProviderRef;

        dbConnPool = dbConnPoolRef;
    }

    public Map<String, ApiCallDescriptor> getApiCallDescriptors()
    {
        Map<String, ApiCallDescriptor> objMap;

        Lock readLock = apiLock.readLock();
        try
        {
            readLock.lock();
            objMap = new TreeMap<>(apiCallDescriptors);
        }
        finally
        {
            readLock.unlock();
        }
        return objMap;
    }

    @Override
    public void processMessage(Message msg, TcpConnector connector, Peer client)
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

    private void handleDataMessage(Message msg, TcpConnector connector, Peer client)
        throws IllegalMessageStateException
    {
        try
        {
            byte[] msgData = msg.getData();
            ByteArrayInputStream msgDataIn = new ByteArrayInputStream(msgData);

            MsgHeader header = MsgHeader.parseDelimitedFrom(msgDataIn);
            if (header != null)
            {
                int msgId = header.getMsgId();
                String apiCallName = header.getApiCall();

                Lock readLock = apiLock.readLock();
                Provider<ApiCall> apiCallProvider;
                ApiCallDescriptor apiCallDescriptor;
                try
                {
                    readLock.lock();
                    apiCallProvider = apiCallProviders.get(apiCallName);
                    apiCallDescriptor = apiCallDescriptors.get(apiCallName);
                }
                finally
                {
                    readLock.unlock();
                }
                if (apiCallProvider != null && apiCallDescriptor != null)
                {
                    ApiCallInvocation apiCallInv = new ApiCallInvocation(
                        apiCallProvider,
                        apiCallDescriptor,
                        client.getAccessContext(),
                        errorLog,
                        msg, msgId, msgDataIn,
                        client
                    );
                    workQ.submit(apiCallInv);
                }
                else
                {
                    if (client.getNode() == null) // client, no satellite or controller
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
                        // answer the client
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        MsgHeader.newBuilder()
                            .setApiCall(ApiConsts.API_REPLY)
                            .setMsgId(msgId)
                            .build()
                            .writeDelimitedTo(baos);

                        MsgApiCallResponse.newBuilder()
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


    class ApiCallInvocation implements Runnable
    {
        private final Provider<ApiCall> apiCallProvider;
        private final ApiCallDescriptor apiCallDescriptor;
        private final AccessContext   accCtx;
        private final ErrorReporter   errLog;
        private final Message         msg;
        private final int             msgId;
        private final InputStream     msgDataIn;
        private final Peer            client;

        ApiCallInvocation(
            Provider<ApiCall> apiCallProviderRef,
            ApiCallDescriptor apiCallDescriptorRef, AccessContext accCtxRef,
            ErrorReporter errLogRef,
            Message msgRef,
            int msgIdRef,
            InputStream msgDataInRef,
            Peer clientRef
        )
        {
            apiCallProvider   = apiCallProviderRef;
            apiCallDescriptor = apiCallDescriptorRef;
            accCtx       = accCtxRef;
            errLog       = errLogRef;
            msg          = msgRef;
            msgId        = msgIdRef;
            msgDataIn    = msgDataInRef;
            client       = clientRef;
        }

        @Override
        public void run()
        {
            TransactionMgr transMgr = null;
            transMgr = transactionProvider.get();
            apiCallScope.enter();
            try
            {
                apiCallScope.seed(Key.get(AccessContext.class, PeerContext.class), accCtx);
                apiCallScope.seed(Peer.class, client);
                apiCallScope.seed(Message.class, msg);
                apiCallScope.seed(Key.get(Integer.class, Names.named(ApiModule.MSG_ID)), msgId);
                apiCallScope.seed(TransactionMgr.class, transMgr);
                ApiCall apiCall = apiCallProvider.get();
                apiCall.execute(msgDataIn);
            }
            catch (Exception | ImplementationError exc)
            {
                errLog.reportError(
                    Level.ERROR,
                    exc,
                    accCtx,
                    client,
                    "Error occurred while executing the '" + apiCallDescriptor.getName() + "' API."
                );
            }
            finally
            {
                if (transMgr != null)
                {
                    dbConnPool.returnConnection(transMgr);
                }
                apiCallScope.exit();
            }
        }
    }
}
