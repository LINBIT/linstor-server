package com.linbit.linstor.api;

import java.io.InputStream;

import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

/**
 * Controller / Satellite API call
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ApiCall
{
    /**
     * Returns the name of the API call
     *
     * @return Name of the API call
     */
    String getName();

    /**
     * Returns the description of the API call's function
     *
     * @return Description of the API call's function
     */
    String getDescription();

    /**
     * Executes the API call
     *
     * @param accCtx    The security context to use for execution
     * @param msg       The inbound message that triggered the call
     * @param msgId     The message id from the message's header field
     * @param msgDataIn The input stream containing serialized parameters to the call
     * @param client    The peer that requested the API call
     */
    void execute(
        AccessContext   accCtx,
        Message         msg,
        int             msgId,
        InputStream     msgDataIn,
        Peer            client
    );
}
