package com.linbit.linstor.api.protobuf;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.layer.storage.DevicePoolHandler;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.layer.storage.utils.SEDUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.c2s.MsgCreateDevicePoolOuterClass.MsgCreateDevicePool;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.RaidLevel;

import javax.annotation.Nullable;
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
    private final Provider<Long> apiCallId;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final DevicePoolHandler devicePoolHandler;
    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;
    private final StltExtToolsChecker stltExtToolsChecker;

    @Inject
    public CreateDevicePool(
        Provider<Peer> peerProviderRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        DevicePoolHandler devicePoolHandlerRef,
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        StltExtToolsChecker stltExtToolsCheckerRef
    )
    {
        this.peerProvider = peerProviderRef;
        this.apiCallId = apiCallIdRef;
        this.ctrlStltSerializer = ctrlStltSerializerRef;
        this.devicePoolHandler = devicePoolHandlerRef;
        this.errorReporter = errorReporterRef;
        this.extCmdFactory = extCmdFactoryRef;
        stltExtToolsChecker = stltExtToolsCheckerRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        LvmUtils.recacheNext();
        MsgCreateDevicePool msgCreateDevicePool = MsgCreateDevicePool.parseDelimitedFrom(msgDataIn);

        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        DeviceProviderKind kind = ProtoDeserializationUtils.parseDeviceProviderKind(
            msgCreateDevicePool.getProviderKind());

        apiCallRc.addEntries(devicePoolHandler.checkPoolExists(kind, msgCreateDevicePool.getPoolName()));

        if (msgCreateDevicePool.hasVdoArguments() && kind == DeviceProviderKind.LVM_THIN)
        {
            @Nullable ExtToolsInfo lvmInfo = stltExtToolsChecker.getExternalTools(false).get(ExtTools.LVM);
            if (lvmInfo != null && !lvmInfo.getVersion().greaterOrEqual(new ExtToolsInfo.Version(2, 3, 24)))
            {
                ApiCallRcImpl.ApiCallRcEntry entry =
                    ApiCallRcImpl.entryBuilder(
                            ApiConsts.FAIL_INVLD_CONF,
                            "LVM version does not support VDO with LVM-THIN")
                        .setSkipErrorReport(true)
                        .setCause("VDO with LVM-THIN needs at least LVM 2.3.24")
                        .build();
                apiCallRc.add(entry);
            }
        }

        if (!apiCallRc.hasErrors())
        {
            final List<String> devicePaths = msgCreateDevicePool.getDevicePathsList();

            // check if sed enabled and prepare sed device
            if (msgCreateDevicePool.getSed())
            {
                final List<String> sedPasswords = msgCreateDevicePool.getSedPasswordsList();
                for (int idx = 0; idx < devicePaths.size(); idx++)
                {
                    String devicePath = devicePaths.get(idx);
                    String sedPassword = sedPasswords.get(idx);
                    String realPath = SEDUtils.realpath(errorReporter, devicePath);
                    if (SEDUtils.initializeSED(
                        extCmdFactory,
                        errorReporter,
                        apiCallRc,
                        realPath,
                        sedPassword))
                    {
                        SEDUtils.unlockSED(extCmdFactory, errorReporter, realPath, sedPassword);
                    }
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
                        msgCreateDevicePool.getPoolName() + InternalApiConsts.VDO_POOL_SUFFIX));

                devicePoolHandler.createVdoDevice(
                    apiCallRc,
                    kind,
                    msgCreateDevicePool.getPoolName() + InternalApiConsts.VDO_POOL_SUFFIX,
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
            final List<String> sedPasswords = msgCreateDevicePool.getSedPasswordsList();
            for (int idx = 0; idx < devicePaths.size(); idx++)
            {
                String devicePath = devicePaths.get(idx);
                String sedPassword = sedPasswords.get(idx);
                // try revert sed locking
                String realPath = SEDUtils.realpath(errorReporter, devicePath);
                SEDUtils.revertSEDLocking(
                    extCmdFactory, errorReporter, realPath, sedPassword);
            }
        }

        peerProvider.get().sendMessage(
            ctrlStltSerializer.answerBuilder(ApiConsts.API_REPLY, apiCallId.get()).apiCallRcSeries(apiCallRc).build(),
            ApiConsts.API_REPLY
        );
    }
}
