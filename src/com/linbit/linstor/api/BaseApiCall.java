package com.linbit.linstor.api;

import com.linbit.ImplementationError;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

import java.io.IOException;
import java.io.InputStream;
import org.slf4j.event.Level;

/**
 * Base class for network APIs
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class BaseApiCall implements ApiCall
{
    protected final ErrorReporter errorReporter;

    public BaseApiCall(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
    }

    @Override
    public void execute(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
    {
        try
        {
            executeImpl(accCtx, msg, msgId, msgDataIn, client);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(
                Level.ERROR,
                ioExc,
                accCtx,
                client,
                "IO error occured while executing the '" + getName() + "' API."
            );
        }
    }

    protected abstract void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException;

    protected void answerApiCallRc(
        AccessContext accCtx,
        Peer peer,
        int msgId,
        ApiCallRc apiCallRc
    )
    {
        byte[] apiCallMsgData = createApiCallResponse(accCtx, apiCallRc, peer);
        byte[] apiCallData = prepareMessage(accCtx, apiCallMsgData, peer, msgId, ApiConsts.API_REPLY);

        sendAnswer(peer, apiCallData);
    }

    protected void sendAnswer(
        Peer peer,
        byte[] preparedMsgData
    )
    {
        Message apiCallAnswer = peer.createMessage();

        try
        {
            apiCallAnswer.setData(preparedMsgData);
        }
        catch (IllegalMessageStateException illegalMsgStateExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Setting the byte[] data for a new message failed",
                    illegalMsgStateExc
                )
            );
        }

        try
        {
            peer.sendMessage(apiCallAnswer);
        }
        catch (IllegalMessageStateException illegalMsgStateExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Sending a new message failed",
                    illegalMsgStateExc
                )
            );
        }
    }

    protected abstract byte[] createApiCallResponse(
        AccessContext accCtx,
        ApiCallRc apiCallRc,
        Peer peer
    );

    /**
     * Prepare a message header for the given msgId and the apicalltype.
     *
     * @param accCtx AccessContext used for error reporting.
     * @param msgsBytes Msgs that should be added after the header.
     * @param peer Peer to send the message too, for error reporting.
     * @param msgId Message id to use.
     * @param apicalltype Api call type.
     * @return A new byte array with the correct header and appended MsgsBytes.
     */
    protected abstract byte[] prepareMessage(
        AccessContext accCtx,
        byte[] msgsBytes,
        Peer peer,
        int msgId,
        String apicalltype
    );
}
