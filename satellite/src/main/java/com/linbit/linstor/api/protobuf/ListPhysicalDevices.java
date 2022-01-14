package com.linbit.linstor.api.protobuf;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.layer.storage.utils.LsBlkUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.c2s.MsgReqPhysicalDevicesOuterClass;
import com.linbit.linstor.storage.LsBlkEntry;
import com.linbit.linstor.storage.StorageException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@ProtobufApiCall(
    name = InternalApiConsts.API_LIST_PHYSICAL_DEVICES,
    description = "Returns lsblk output to the controller"
)
@Singleton
public class ListPhysicalDevices implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;
    private final Provider<Peer> peerProvider;
    private Provider<Long> apiCallId;
    private final CtrlStltSerializer ctrlStltSerializer;

    @Inject
    public ListPhysicalDevices(
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        Provider<Peer> peerProviderRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
        CtrlStltSerializer ctrlStltSerializerRef
    )
    {
        errorReporter = errorReporterRef;
        extCmdFactory = extCmdFactoryRef;
        peerProvider = peerProviderRef;
        apiCallId = apiCallIdRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        try
        {
            MsgReqPhysicalDevicesOuterClass.MsgReqPhysicalDevices msgReqPhysicalDevices =
                MsgReqPhysicalDevicesOuterClass.MsgReqPhysicalDevices.parseDelimitedFrom(msgDataIn);

            List<LsBlkEntry> entries = LsBlkUtils.lsblk(extCmdFactory.create());

            if (msgReqPhysicalDevices.getFilter())
            {
                entries = LsBlkUtils.filterDeviceCandidates(entries);
            }

            byte[] answer = ctrlStltSerializer
                .answerBuilder(InternalApiConsts.API_ANSWER_PHYSICAL_DEVICES, apiCallId.get())
                .physicalDevices(entries)
                .build();

            peerProvider.get().sendMessage(answer, InternalApiConsts.API_ANSWER_PHYSICAL_DEVICES);
        }
        catch (StorageException stoExc)
        {
            errorReporter.reportError(stoExc);
        }
    }
}
