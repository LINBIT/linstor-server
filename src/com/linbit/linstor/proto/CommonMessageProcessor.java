package com.linbit.linstor.proto;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.event.Level;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.WorkQueue;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
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

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Dispatcher for received messages
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class CommonMessageProcessor implements MessageProcessor
{
    private final TreeMap<String, ApiCall> apiCallMap;
    private final ReadWriteLock apiLock;

    private final ErrorReporter errorLog;
    private final WorkQueue workQ;

    @Inject
    public CommonMessageProcessor(ErrorReporter errorLogRef, WorkQueue workQRef)
    {
        ErrorCheck.ctorNotNull(CommonMessageProcessor.class, WorkQueue.class, workQRef);
        apiCallMap  = new TreeMap<>();
        apiLock     = new ReentrantReadWriteLock();
        errorLog    = errorLogRef;
        workQ       = workQRef;
    }

    public void addApiCall(ApiCall apiCallObj)
    {
        Lock writeLock = apiLock.writeLock();
        try
        {
            writeLock.lock();
            apiCallMap.put(apiCallObj.getName(), apiCallObj);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public void removeApiCall(String apiCallName)
    {
        Lock writeLock = apiLock.writeLock();
        try
        {
            writeLock.lock();
            apiCallMap.remove(apiCallName);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public void clearApiCalls()
    {
        Lock writeLock = apiLock.writeLock();
        try
        {
            writeLock.lock();
            apiCallMap.clear();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    public Set<String> getApiCallNames()
    {
        Lock readLock = apiLock.readLock();
        Set<String> apiNames;
        try
        {
            readLock.lock();
            apiNames = new TreeSet<>();
            apiNames.addAll(apiCallMap.keySet());
        }
        finally
        {
            readLock.unlock();
        }
        return apiNames;
    }

    public Map<String, ApiCall> getApiCallObjects()
    {
        Lock readLock = apiLock.readLock();
        Map<String, ApiCall> objList;
        try
        {
            readLock.lock();
            objList = new TreeMap<>();
            for (ApiCall apiObj : apiCallMap.values())
            {
                objList.put(apiObj.getName(), apiObj);
            }
        }
        finally
        {
            readLock.unlock();
        }
        return objList;
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
                ApiCall apiCallObj;
                try
                {
                    readLock.lock();
                    apiCallObj = apiCallMap.get(apiCallName);
                }
                finally
                {
                    readLock.unlock();
                }
                if (apiCallObj != null)
                {
                    ApiCallInvocation apiCallInv = new ApiCallInvocation(
                        apiCallObj,
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


    static class ApiCallInvocation implements Runnable
    {
        private ApiCall         apiCallObj;
        private AccessContext   accCtx;
        private ErrorReporter   errLog;
        private Message         msg;
        private int             msgId;
        private InputStream     msgDataIn;
        private Peer            client;

        ApiCallInvocation(
            ApiCall         apiCallRef,
            AccessContext   accCtxRef,
            ErrorReporter   errLogRef,
            Message         msgRef,
            int             msgIdRef,
            InputStream     msgDataInRef,
            Peer            clientRef
        )
        {
            apiCallObj  = apiCallRef;
            accCtx      = accCtxRef;
            errLog      = errLogRef;
            msg         = msgRef;
            msgId       = msgIdRef;
            msgDataIn   = msgDataInRef;
            client      = clientRef;
        }

        @Override
        public void run()
        {
            try
            {
                apiCallObj.execute(accCtx, msg, msgId, msgDataIn, client);
            }
            catch (Exception | ImplementationError exc)
            {
                errLog.reportError(Level.ERROR, exc, accCtx, client, null);
            }
        }
    }
}
