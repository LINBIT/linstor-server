package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.netcom.Peer;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_FULL_SYNC_FAILED,
    description = "Satellite failed to apply our full sync",
    transactional = false
)
@Singleton
public class IntFullSyncFailed implements ApiCall
{
    private final Provider<Peer> satelliteProvider;

    @Inject
    public IntFullSyncFailed(Provider<Peer> satelliteProviderRef)
    {
        satelliteProvider = satelliteProviderRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        satelliteProvider.get().fullSyncFailed();
    }

}
