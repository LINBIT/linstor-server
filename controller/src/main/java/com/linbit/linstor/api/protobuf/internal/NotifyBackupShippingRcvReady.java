package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LDstApiCallHandler;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntBackupRcvReadyOuterClass.MsgIntBackupRcvReady;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_BACKUP_RCV_READY,
    description = "Called by the satellite to notify the controller that it is ready to receive the backup",
    transactional = true
)
@Singleton
public class NotifyBackupShippingRcvReady implements ApiCallReactive
{
    private CtrlBackupL2LDstApiCallHandler ctrlBackupL2LDstApiCallHandler;

    @Inject
    public NotifyBackupShippingRcvReady(
        CtrlBackupL2LDstApiCallHandler ctrlBackupApiCallHandlerRef
    )
    {
        ctrlBackupL2LDstApiCallHandler = ctrlBackupApiCallHandlerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataInRef) throws IOException
    {
        MsgIntBackupRcvReady ship = MsgIntBackupRcvReady.parseDelimitedFrom(msgDataInRef);
        return ctrlBackupL2LDstApiCallHandler.sendBackupShippingReceiveRequest(
            ship.getRscName(),
            ship.getSnapName(),
            ship.getNodeName(),
            ship.getRemoteName()
        ).thenMany(Flux.<byte[]> empty());
    }
}
