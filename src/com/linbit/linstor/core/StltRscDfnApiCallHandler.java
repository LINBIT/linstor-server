package com.linbit.linstor.core;

import com.linbit.linstor.security.AccessContext;

class StltRscDfnApiCallHandler
{
    private Satellite satelliteRef;
    private AccessContext apiCtx;

    public StltRscDfnApiCallHandler(Satellite satelliteRef, AccessContext apiCtx)
    {
        this.satelliteRef = satelliteRef;
        this.apiCtx = apiCtx;
    }
}
