package com.linbit.linstor.netcom;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;

public class PeerController extends PeerOffline
{
    private final ApiConsts.ConnectionStatus status;

    public PeerController(ErrorReporter errorReporterRef, String peerIdRef, Node nodeRef, boolean local)
    {
        super(errorReporterRef, peerIdRef, nodeRef);
        if (local)
        {
            status = ApiConsts.ConnectionStatus.ONLINE;
        }
        else
        {
            status = ApiConsts.ConnectionStatus.OFFLINE;
        }
    }

    @Override
    public ApiConsts.ConnectionStatus getConnectionStatus()
    {
        return status;
    }

}
