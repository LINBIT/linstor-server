package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.FreeSpacePojo;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.MsgIntFullSyncSuccessOuterClass.MsgIntFullSyncSuccess;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_FULL_SYNC_SUCCESS,
    description = "Satellite failed to apply our full sync"
)
public class IntFullSyncSuccess implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Peer satellite;

    @Inject
    public IntFullSyncSuccess(
        CtrlApiCallHandler apiCallHandlerRef,
        Peer satelliteRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        satellite = satelliteRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntFullSyncSuccess msgIntFullSyncSuccess = MsgIntFullSyncSuccess.parseDelimitedFrom(msgDataIn);

        List<FreeSpacePojo> freeSpacePojoList = new ArrayList<>();
        for (StorPoolFreeSpace protoFreeSpace : msgIntFullSyncSuccess.getFreeSpaceList())
        {
            freeSpacePojoList.add(
                new FreeSpacePojo(
                    UUID.fromString(protoFreeSpace.getStorPoolUuid()),
                    protoFreeSpace.getStorPoolName(),
                    protoFreeSpace.getFreeSpace()
                )
            );
        }
        FreeSpacePojo[] freeSpacePojos = new FreeSpacePojo[freeSpacePojoList.size()];
        freeSpacePojoList.toArray(freeSpacePojos);
        apiCallHandler.updateRealFreeSpace(satellite, freeSpacePojos);
        satellite.setConnectionStatus(Peer.ConnectionStatus.ONLINE);
    }

}
