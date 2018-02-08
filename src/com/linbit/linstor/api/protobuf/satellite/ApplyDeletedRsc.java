package com.linbit.linstor.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntRscDeletedDataOuterClass.MsgIntRscDeletedData;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ApplyDeletedRsc extends BaseProtoApiCall
{
    private final Satellite satellite;

    public ApplyDeletedRsc(Satellite satelliteRef)
    {
        super(satelliteRef.getErrorReporter());
        satellite = satelliteRef;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_APPLY_RSC_DELETED;
    }

    @Override
    public String getDescription()
    {
        return "Applies an update of a deleted resource (ensuring the resource is deleted)";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgIntRscDeletedData rscDeletedData = MsgIntRscDeletedData.parseDelimitedFrom(msgDataIn);
        satellite.getApiCallHandler().applyDeletedResourceChange(
            rscDeletedData.getRscName()
        );
    }
}
