package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.layer.storage.DevicePoolHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.c2s.MsgCreateDevicePoolOuterClass.MsgCreateDevicePool;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.RaidLevel;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@ProtobufApiCall(
    name = InternalApiConsts.API_CREATE_DEVICE_POOL,
    description = "Creates a device pool"
)
@Singleton
public class CreateDevicePool implements ApiCall
{
    private final Provider<Peer> peerProvider;
    private Provider<Long> apiCallId;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final DevicePoolHandler devicePoolHandler;

    @Inject
    public CreateDevicePool(
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
        MsgCreateDevicePool msgCreateDevicePool = MsgCreateDevicePool.parseDelimitedFrom(msgDataIn);

        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        DeviceProviderKind kind = ProtoDeserializationUtils.parseDeviceProviderKind(
            msgCreateDevicePool.getProviderKind());

        apiCallRc.addEntries(devicePoolHandler.checkPoolExists(kind, msgCreateDevicePool.getPoolName()));

        if (!apiCallRc.hasErrors())
        {
            List<String> devicePaths = msgCreateDevicePool.getDevicePathsList();
            ArrayList<String> lvmDevicePaths = new ArrayList<>(devicePaths);
            if (msgCreateDevicePool.hasVdoArguments())
            {
                lvmDevicePaths.clear();
                for (final String rawDevicePath : devicePaths)
                {
                    lvmDevicePaths.add(devicePoolHandler.createVdoDevice(
                        apiCallRc,
                        rawDevicePath,
                        msgCreateDevicePool.getPoolName(),
                        msgCreateDevicePool.getLogicalSizeKib(),
                        msgCreateDevicePool.getVdoArguments().getSlabSizeKib()
                    ));
                }
            }

            if (!lvmDevicePaths.isEmpty() && !apiCallRc.hasErrors())
            {
                apiCallRc.addEntries(devicePoolHandler.createDevicePool(
                    kind,
                    lvmDevicePaths,
                    RaidLevel.valueOf(msgCreateDevicePool.getRaidLevel().name()),
                    msgCreateDevicePool.getPoolName()
                ));
            }
        }

        peerProvider.get().sendMessage(
            ctrlStltSerializer.answerBuilder(ApiConsts.API_REPLY, apiCallId.get()).apiCallRcSeries(apiCallRc).build(),
            ApiConsts.API_REPLY
        );
    }
}
