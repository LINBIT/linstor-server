package com.linbit.linstor.api.protobuf.controller.serializer;

import java.util.UUID;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public class StorPoolDataSerializerProto extends AbsSerializerProto<StorPool>
{
    public StorPoolDataSerializerProto(AccessContext serializerCtx, ErrorReporter errorReporter)
    {
        super(
            serializerCtx,
            errorReporter,
            InternalApiConsts.API_CHANGED_STOR_POOL,
            InternalApiConsts.API_APPLY_STOR_POOL
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
    protected MsgIntStorPoolData buildData(StorPool storPool) throws AccessDeniedException
    {
        StorPoolDefinition storPoolDfn = storPool.getDefinition(serializerCtx);
        return MsgIntStorPoolData.newBuilder()
            .setStorPoolUuid(asByteString(storPool.getUuid()))
            .setNodeUuid(asByteString(storPool.getNode().getUuid()))
            .setStorPoolDfnUuid(asByteString(storPoolDfn.getUuid()))
            .setStorPoolName(storPool.getName().displayValue)
            .setDriver(storPool.getDriverName())
            .addAllStorPoolProps(asLinStorList(storPool.getProps(serializerCtx)))
            .addAllStorPoolDfnProps(asLinStorList(storPoolDfn.getProps(serializerCtx)))
            .build();
    }
}
