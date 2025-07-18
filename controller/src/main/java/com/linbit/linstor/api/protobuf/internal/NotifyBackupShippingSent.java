package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlBackupShippingSentInternalCallHandler;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntBackupShippedOuterClass.MsgIntBackupShipped;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_SENT,
    description = "Called by the satellite to notify the controller that a backup has been sent",
    transactional = true
)
@Singleton
public class NotifyBackupShippingSent implements ApiCallReactive
{
    private final CtrlBackupShippingSentInternalCallHandler ctrlBackupShippingSentHandler;

    @Inject
    public NotifyBackupShippingSent(
        CtrlBackupShippingSentInternalCallHandler ctrlBackupShippingSentHandlerRef
    )
    {
        ctrlBackupShippingSentHandler = ctrlBackupShippingSentHandlerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataInRef) throws IOException
    {
        MsgIntBackupShipped ship = MsgIntBackupShipped.parseDelimitedFrom(msgDataInRef);
        // ship.getPortsList is being ignored since the sending side does not care about ports
        return ctrlBackupShippingSentHandler.shippingSent(
            ship.getRscName(),
            ship.getSnapName(),
            ship.getSuccess(),
            ship.getRemoteName()
        ).thenMany(Flux.<byte[]>empty());
    }
}
