package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgLstKvsPropsOuterClass.MsgLstKvsProps;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
@ProtobufApiCall(
    name = ApiConsts.API_LST_KVS_PROPS,
    description = "Queries the list of properties of a given instance name"
)
@Singleton
public class ListKvsProps implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Provider<Peer> clientProvider;
    private final Provider<Long> apiCallId;
    private final CtrlClientSerializer ctrlClientSerializer;

    @Inject
    public ListKvsProps(
        CtrlApiCallHandler apiCallHandlerRef,
        Provider<Peer> clientProviderRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
        CtrlClientSerializer ctrlClientSerializerRef)
    {
        apiCallHandler = apiCallHandlerRef;
        clientProvider = clientProviderRef;
        apiCallId = apiCallIdRef;
        ctrlClientSerializer = ctrlClientSerializerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgLstKvsProps msgLstProps = MsgLstKvsProps.parseDelimitedFrom(msgDataIn);

        clientProvider.get().sendMessage(
            ctrlClientSerializer
                .answerBuilder(
                    ApiConsts.API_LST_KVS_PROPS,
                    apiCallId.get()
                )
                .keyValueStoreList(
                    apiCallHandler.listKvsProps(
                        msgLstProps.getInstanceName()
                    )
                )
                .build()
        );
    }
}
