package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LDstApiCallHandler;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntBackupShippingWrongPortsOuterClass.MsgIntBackupShippingWrongPorts;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.TreeSet;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_WRONG_PORTS,
    description = "Called by the satellite to notify the controller that some ports were already in use",
    transactional = true
)
@Singleton
public class NotifyBackupShippingWrongPorts implements ApiCallReactive
{
    private CtrlBackupL2LDstApiCallHandler ctrlBackupL2LDstApiCallHandler;

    @Inject
    public NotifyBackupShippingWrongPorts(
        CtrlBackupL2LDstApiCallHandler ctrlBackupApiCallHandlerRef
    )
    {
        ctrlBackupL2LDstApiCallHandler = ctrlBackupApiCallHandlerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataInRef) throws IOException
    {
        MsgIntBackupShippingWrongPorts ship = MsgIntBackupShippingWrongPorts.parseDelimitedFrom(msgDataInRef);
        return ctrlBackupL2LDstApiCallHandler.reallocatePorts(
            ship.getRemoteName(),
            ship.getSnapName(),
            ship.getRscName(),
            new TreeSet<>(ship.getPortsList())
        ).thenMany(Flux.<byte[]> empty());
    }
}
