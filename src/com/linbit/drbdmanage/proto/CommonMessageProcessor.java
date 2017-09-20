package com.linbit.drbdmanage.proto;

import com.linbit.ErrorCheck;
import com.linbit.WorkQueue;
import com.linbit.drbdmanage.ApiCall;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.MessageProcessor;
import com.linbit.drbdmanage.netcom.MessageTypes;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnector;
import com.linbit.drbdmanage.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.drbdmanage.security.AccessContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Dispatcher for received messages
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CommonMessageProcessor implements MessageProcessor
{
    private final TreeMap<String, ApiCall> apiCallMap;
    private final ReadWriteLock apiLock;

    private final CoreServices    coreSvcs;
    private final WorkQueue       workQ;

    public CommonMessageProcessor(CoreServices coreSvcsRef, WorkQueue workQRef)
    {
        ErrorCheck.ctorNotNull(CommonMessageProcessor.class, WorkQueue.class, workQRef);
        apiCallMap  = new TreeMap<>();
        apiLock     = new ReentrantReadWriteLock();
        coreSvcs    = coreSvcsRef;
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

    @Override
    public void processMessage(Message msg, TcpConnector connector, Peer client)
    {
        try
        {
            switch(msg.getType())
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
            }
        }
        catch (IllegalMessageStateException msgExc)
        {
            coreSvcs.getErrorReporter().reportError(msgExc);
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
                    msg, msgId, msgDataIn,
                    connector, client
                );
                workQ.submit(apiCallInv);
            }
            else
            {
                // FIXME: Debug code
                System.err.printf("Unknown API call '%s'\n", apiCallName);
            }
        }
        catch (IOException ioExc)
        {
            // TODO: - Send an error report to the peer that sent the faulty message
            //       - Probably only from the controller to a satellite or client,
            //         and from the satellite to a client; otherwise, this can create
            //         a loop if error reports are created due to receiving a
            //         malformed error report
            coreSvcs.getErrorReporter().reportError(ioExc);
        }
    }

    static class ApiCallInvocation implements Runnable
    {
        private ApiCall         apiCallObj;
        private AccessContext   accCtx;
        private Message         msg;
        private int             msgId;
        private InputStream     msgDataIn;
        private TcpConnector    connector;
        private Peer            client;

        ApiCallInvocation(
            ApiCall         apiCallRef,
            AccessContext   accCtxRef,
            Message         msgRef,
            int             msgIdRef,
            InputStream     msgDataInRef,
            TcpConnector    connectorRef,
            Peer            clientRef
        )
        {
            apiCallObj  = apiCallRef;
            accCtx      = accCtxRef;
            msg         = msgRef;
            msgId       = msgIdRef;
            msgDataIn   = msgDataInRef;
            connector   = connectorRef;
            client      = clientRef;
        }

        @Override
        public void run()
        {
            apiCallObj.execute(accCtx, msg, msgId, msgDataIn, connector, client);
        }
    }
}
