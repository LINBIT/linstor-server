package com.linbit.linstor.api.protobuf.satellite;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntControllerDataOuterClass.MsgIntControllerData;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_CONTROLLER,
    description = "Applies controller update data"
)
public class ApplyController implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;

    @Inject
    public ApplyController(StltApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntControllerData controllerData = MsgIntControllerData.parseDelimitedFrom(msgDataIn);
        apiCallHandler.applyControllerChanges(
            ProtoMapUtils.asMap(controllerData.getControllerPropsList()),
            controllerData.getFullSyncId(),
            controllerData.getUpdateId()
        );
    }
}
