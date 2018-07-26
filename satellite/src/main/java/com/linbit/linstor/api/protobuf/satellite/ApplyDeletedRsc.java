package com.linbit.linstor.api.protobuf.satellite;

import javax.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntRscDeletedDataOuterClass.MsgIntRscDeletedData;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_RSC_DELETED,
    description = "Applies an update of a deleted resource (ensuring the resource is deleted)"
)
public class ApplyDeletedRsc implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;

    @Inject
    public ApplyDeletedRsc(StltApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntRscDeletedData rscDeletedData = MsgIntRscDeletedData.parseDelimitedFrom(msgDataIn);
        apiCallHandler.applyDeletedResourceChange(
            rscDeletedData.getRscName(),
            rscDeletedData.getFullSyncId(),
            rscDeletedData.getUpdateId()
        );
    }
}
