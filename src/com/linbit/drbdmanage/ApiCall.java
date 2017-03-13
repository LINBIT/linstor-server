package com.linbit.drbdmanage;

import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnector;
import com.linbit.drbdmanage.security.AccessContext;
import java.io.InputStream;

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
     * Executes the API call
     *
     * @param accCtx    The security context to use for execution
     * @param msg       The inbound message that triggered the call
     * @param msgId     The message id from the message's header field
     * @param msgDataIn The input stream containing serialized parameters to the call
     * @param connector The connector that manages the peer's connection
     * @param client    The peer that requested the API call
     */
    void execute(
        AccessContext   accCtx,
        Message         msg,
        int             msgId,
        InputStream     msgDataIn,
        TcpConnector    connector,
        Peer            client
    );
}
