package com.linbit.linstor.api.protobuf.controller;

import com.google.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.netcom.Peer;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_FULL_SYNC_FAILED,
    description = "Satellite failed to apply our full sync"
)
public class IntFullSyncFailed implements ApiCall
{
    private final Peer satellite;

    @Inject
    public IntFullSyncFailed(Peer satelliteRef)
    {
        satellite = satelliteRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        satellite.fullSyncFailed();
    }

}
