package com.linbit.drbdmanage.core;

import java.util.Map;

import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;

class CtrlVlmConnectionApiCallHandler
{
    private Controller controller;

    CtrlVlmConnectionApiCallHandler(Controller controllerRef)
    {
        controller = controllerRef;
    }

    public ApiCallRc createVolumeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        String rscName,
        int vlmNr,
        Map<String, String> vlmConnPropsMap
    )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public ApiCallRc deleteVolumeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        String rscName,
        int vlmNr
    )
    {
        // TODO Auto-generated method stub
        return null;
    }

}
