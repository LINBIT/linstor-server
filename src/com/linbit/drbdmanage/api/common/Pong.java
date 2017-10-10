package com.linbit.drbdmanage.api.common;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;

public class Pong extends BaseApiCall
{
    public Pong(Controller controller)
    {
        super(controller.getErrorReporter());
    }

    @Override
    public String getName()
    {
        return Pong.class.getSimpleName();
    }

    @Override
    public void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        client.pongReceived();
    }

}
