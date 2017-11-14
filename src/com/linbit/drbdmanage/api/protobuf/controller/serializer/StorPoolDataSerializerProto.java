package com.linbit.drbdmanage.api.protobuf.controller.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;

import com.linbit.drbdmanage.InternalApiConsts;
import com.linbit.drbdmanage.StorPool;
import com.linbit.drbdmanage.StorPoolDefinition;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

public class StorPoolDataSerializerProto extends AbsSerializerProto<StorPool>
{
    public StorPoolDataSerializerProto(AccessContext serializerCtx, ErrorReporter errorReporter)
    {
        super(
            serializerCtx,
            errorReporter,
            InternalApiConsts.API_STOR_POOL_CHANGED,
            InternalApiConsts.API_STOR_POOL_DATA
        );
    }

    @Override
    protected String getName(StorPool storPool)
    {
        return storPool.getName().displayValue;
    }

    @Override
    protected UUID getUuid(StorPool storPool)
    {
        return storPool.getUuid();
    }

    @Override
    protected void writeData(StorPool storPool, ByteArrayOutputStream baos)
        throws IOException, AccessDeniedException
    {
        StorPoolDefinition storPoolDfn = storPool.getDefinition(serializerCtx);
        MsgIntStorPoolData.newBuilder()
            .setStorPoolUuid(asByteString(storPool.getUuid()))
            .setNodeUuid(asByteString(storPool.getNode().getUuid()))
            .setStorPoolDfnUuid(asByteString(storPoolDfn.getUuid()))
            .setStorPoolName(storPool.getName().displayValue)
            .setDriver(storPool.getDriverName())
            .addAllStorPoolProps(asLinStorList(storPool.getConfiguration(serializerCtx)))
            .addAllStorPoolDfnProps(asLinStorList(storPoolDfn.getConfiguration(serializerCtx)))
            .build()
            .writeDelimitedTo(baos);
    }

}
