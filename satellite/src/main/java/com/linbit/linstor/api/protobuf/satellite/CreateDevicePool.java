package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.c2s.MsgCreateDevicePoolOuterClass.MsgCreateDevicePool;
import com.linbit.linstor.storage.DevicePoolHandler;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_CREATE_DEVICE_POOL,
    description = "Applies storage pool update data"
)
@Singleton
public class CreateDevicePool implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final Provider<Peer> peerProvider;
    private Provider<Long> apiCallId;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final DevicePoolHandler devicePoolHandler;

    @Inject
    public CreateDevicePool(
        ErrorReporter errorReporterRef,
        Provider<Peer> peerProviderRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        DevicePoolHandler devicePoolHandlerRef
    )
    {
        this.errorReporter = errorReporterRef;
        this.peerProvider = peerProviderRef;
        this.apiCallId = apiCallIdRef;
        this.ctrlStltSerializer = ctrlStltSerializerRef;
        this.devicePoolHandler = devicePoolHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgCreateDevicePool msgCreateDevicePool = MsgCreateDevicePool.parseDelimitedFrom(msgDataIn);

        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        String devicePath = msgCreateDevicePool.getDevicePath();
        if (msgCreateDevicePool.hasVdoArguments())
        {
            devicePath = devicePoolHandler.createVdoDevice(
                apiCallRc,
                msgCreateDevicePool.getDevicePath(),
                msgCreateDevicePool.getPoolName(),
                msgCreateDevicePool.getLogicalSizeKib(),
                msgCreateDevicePool.getVdoArguments().getSlabSizeKib()
            );
        }

        if (devicePath != null)
        {
            apiCallRc.addEntries(devicePoolHandler.createDevicePool(
                ProtoDeserializationUtils.parseDeviceProviderKind(msgCreateDevicePool.getProviderKind()),
                devicePath,
                msgCreateDevicePool.getPoolName(),
                msgCreateDevicePool.getLogicalSizeKib()
            ));
        }

        peerProvider.get().sendMessage(
            ctrlStltSerializer.answerBuilder(ApiConsts.API_REPLY, apiCallId.get()).apiCallRcSeries(apiCallRc).build()
        );
    }
}
