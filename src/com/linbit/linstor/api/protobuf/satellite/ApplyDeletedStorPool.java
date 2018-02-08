package com.linbit.linstor.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDeletedDataOuterClass.MsgIntStorPoolDeletedData;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ApplyDeletedStorPool extends BaseProtoApiCall
{
    private final Satellite satellite;

    public ApplyDeletedStorPool(Satellite satelliteRef)
    {
        super(satelliteRef.getErrorReporter());
        satellite = satelliteRef;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_APPLY_STOR_POOL_DELETED;
    }

    @Override
    public String getDescription()
    {
        return "Applies an update of a deleted storage pool (ensuring the storage pool is deleted)";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgIntStorPoolDeletedData storPoolDeletedData = MsgIntStorPoolDeletedData.parseDelimitedFrom(msgDataIn);
        satellite.getApiCallHandler().applyDeletedStorPoolChange(
            storPoolDeletedData.getStorPoolName()
        );
    }
}
