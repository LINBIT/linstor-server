package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.SnapshotShippingInternalApiCallHandler;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntSnapshotShippedOuterClass.MsgIntSnapshotShipped;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

import reactor.core.publisher.Flux;

@Deprecated(forRemoval = true)
@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_SNAPSHOT_SHIPPING_RECEIVED,
    description = "Called by the satellite to notify the controller that a snapshot has been received",
    transactional = true
)
@Singleton
public class NotifySnapshotShippingReceived implements ApiCallReactive
{
    private final SnapshotShippingInternalApiCallHandler snapShipIntCallHandler;

    @Inject
    public NotifySnapshotShippingReceived(SnapshotShippingInternalApiCallHandler snapShipIntCallHandlerRef)
    {
        snapShipIntCallHandler = snapShipIntCallHandlerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataInRef) throws IOException
    {
        MsgIntSnapshotShipped ship = MsgIntSnapshotShipped.parseDelimitedFrom(msgDataInRef);
        return snapShipIntCallHandler.shippingReceived(
            ship.getRscName(),
            ship.getSnapName(),
            ship.getSuccess(),
            ship.getVlmNrsWithBlockedPortList()
        ).thenMany(Flux.<byte[]>empty());
    }
}
