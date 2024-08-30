package com.linbit.linstor.api.protobuf;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.layer.storage.DevicePoolHandler;
import com.linbit.linstor.layer.storage.utils.SEDUtils;
import com.linbit.linstor.logging.ErrorReporter;
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
    private static final String VDO_POOL_SUFFIX = "-vdobase";
    private final Provider<Peer> peerProvider;
    private final Provider<Long> apiCallId;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final DevicePoolHandler devicePoolHandler;
    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;

    @Inject
    public CreateDevicePool(
        Provider<Peer> peerProviderRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        DevicePoolHandler devicePoolHandlerRef,
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef
    )
    {
        this.peerProvider = peerProviderRef;
        this.apiCallId = apiCallIdRef;
        this.ctrlStltSerializer = ctrlStltSerializerRef;
        this.devicePoolHandler = devicePoolHandlerRef;
        this.errorReporter = errorReporterRef;
        this.extCmdFactory = extCmdFactoryRef;
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
            final List<String> devicePaths = msgCreateDevicePool.getDevicePathsList();

            // check if sed enabled and prepare sed device
            if (msgCreateDevicePool.getSed())
            {
                for (String devicePath : devicePaths)
                {
                    String realPath = SEDUtils.realpath(errorReporter, devicePath);
                    SEDUtils.initializeSED(
                        extCmdFactory,
                        errorReporter,
                        apiCallRc,
                        realPath,
                        msgCreateDevicePool.getSedPassword());
                    SEDUtils.unlockSED(extCmdFactory, errorReporter, realPath, msgCreateDevicePool.getSedPassword());
                }
            }

            ArrayList<String> lvmDevicePaths = new ArrayList<>(devicePaths);
            if (msgCreateDevicePool.hasVdoArguments())
            {
                apiCallRc.addEntries(
                    devicePoolHandler.createDevicePool(
                        DeviceProviderKind.LVM,
                        devicePaths,
                        RaidLevel.valueOf(msgCreateDevicePool.getRaidLevel().name()),
                        msgCreateDevicePool.getPoolName() + VDO_POOL_SUFFIX));

                devicePoolHandler.createVdoDevice(
                    apiCallRc,
                    msgCreateDevicePool.getPoolName() + VDO_POOL_SUFFIX,
                    msgCreateDevicePool.getPoolName(),
                    msgCreateDevicePool.getLogicalSizeKib(),
                    msgCreateDevicePool.getVdoArguments().getSlabSizeKib()
                );
            }
            else
            {
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
        }

        if (apiCallRc.hasErrors() && msgCreateDevicePool.getSed())
        {
            final List<String> devicePaths = msgCreateDevicePool.getDevicePathsList();
            for (String devicePath : devicePaths)
            {
                // try revert sed locking
                String realPath = SEDUtils.realpath(errorReporter, devicePath);
                SEDUtils.revertSEDLocking(
                    extCmdFactory, errorReporter, realPath, msgCreateDevicePool.getSedPassword());
            }
        }

        peerProvider.get().sendMessage(
            ctrlStltSerializer.answerBuilder(ApiConsts.API_REPLY, apiCallId.get()).apiCallRcSeries(apiCallRc).build(),
            ApiConsts.API_REPLY
        );
    }
}
