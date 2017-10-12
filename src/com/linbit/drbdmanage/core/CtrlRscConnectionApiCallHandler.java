package com.linbit.drbdmanage.core;

import java.util.Map;

import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;

class CtrlRscConnectionApiCallHandler
{
    private Controller controller;

    CtrlRscConnectionApiCallHandler(Controller controllerRef)
    {
        controller = controllerRef;
    }

    public ApiCallRc createResourceConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        String rscName,
        Map<String, String> rscConnPropsMap
    )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public ApiCallRc deleteResourceConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        String rscName
    )
    {
        // TODO Auto-generated method stub
        return null;
    }

}
