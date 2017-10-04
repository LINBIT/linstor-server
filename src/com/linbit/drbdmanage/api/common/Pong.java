package com.linbit.drbdmanage.api.common;

import java.io.InputStream;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;

public class Pong extends BaseApiCall
{
    @Override
    public String getName()
    {
        return Pong.class.getSimpleName();
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
        client.pongReceived();
    }

}
