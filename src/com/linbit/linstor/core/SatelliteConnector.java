package com.linbit.linstor.core;

import com.linbit.linstor.Node;
import com.linbit.linstor.security.AccessContext;

public interface SatelliteConnector
{
    void startConnecting(Node node, AccessContext accCtx);
}
