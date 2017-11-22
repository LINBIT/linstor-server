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
        Message apiCallAnswer = peer.createMessage();

        byte[] apiCallData = getDataForResponse(accCtx, apiCallRc, peer, msgId);

        try
        {
            apiCallAnswer.setData(apiCallData);
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

    protected abstract byte[] getDataForResponse(
        AccessContext accCtx,
        ApiCallRc apiCallRc,
        Peer peer,
        int msgId
    );
}
