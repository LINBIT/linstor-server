package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.layer.storage.DevicePoolHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.c2s.MsgDeleteDevicePoolOuterClass.MsgDeleteDevicePool;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@ProtobufApiCall(
    name = InternalApiConsts.API_DELETE_DEVICE_POOL,
    description = "Deletes a device pool"
)
@Singleton
public class DeleteDevicePool implements ApiCall
{
    private final Provider<Peer> peerProvider;
    private Provider<Long> apiCallId;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final DevicePoolHandler devicePoolHandler;

    @Inject
    public DeleteDevicePool(
        Provider<Peer> peerProviderRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        DevicePoolHandler devicePoolHandlerRef
    )
    {
        this.peerProvider = peerProviderRef;
        this.apiCallId = apiCallIdRef;
        this.ctrlStltSerializer = ctrlStltSerializerRef;
        this.devicePoolHandler = devicePoolHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgDeleteDevicePool msgDeleteDevicePool =
            MsgDeleteDevicePool.parseDelimitedFrom(msgDataIn);

        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        List<String> devicePaths = msgDeleteDevicePool.getDevicePathsList();
        ArrayList<String> lvmDevicePaths = new ArrayList<>(devicePaths);

        if (!lvmDevicePaths.isEmpty())
        {
            apiCallRc.addEntries(devicePoolHandler.deleteDevicePool(
                ProtoDeserializationUtils.parseDeviceProviderKind(msgDeleteDevicePool.getProviderKind()),
                lvmDevicePaths,
                msgDeleteDevicePool.getPoolName()
            ));
        }

        peerProvider.get().sendMessage(
            ctrlStltSerializer.answerBuilder(ApiConsts.API_REPLY, apiCallId.get()).apiCallRcSeries(apiCallRc).build(),
            ApiConsts.API_REPLY
        );
    }
}
