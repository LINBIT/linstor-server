package com.linbit.linstor.netcom;

import com.linbit.Platform;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;

public class PeerController extends PeerOffline
{
    private final ApiConsts.ConnectionStatus status;
    private final @Nullable ApiConsts.Platform platform;
    private final @Nullable String osVariant;

    public PeerController(ErrorReporter errorReporterRef, String peerIdRef, Node nodeRef, boolean local)
    {
        super(errorReporterRef, peerIdRef, nodeRef);
        if (local)
        {
            status = ApiConsts.ConnectionStatus.ONLINE;
            platform = Platform.apiPlatform();
            osVariant = Platform.osVariant();
        }
        else
        {
            status = ApiConsts.ConnectionStatus.OFFLINE;
            platform = null;
            osVariant = null;
        }
    }

    @Override
    public ApiConsts.ConnectionStatus getConnectionStatus()
    {
        return status;
    }

    @Override
    public @Nullable ApiConsts.Platform getPlatform()
    {
        return platform;
    }

    @Override
    public @Nullable String getOsVariant()
    {
        return osVariant;
    }

}
