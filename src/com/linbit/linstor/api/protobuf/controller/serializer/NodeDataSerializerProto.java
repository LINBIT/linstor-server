package com.linbit.linstor.api.protobuf.controller.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public class NodeDataSerializerProto extends AbsSerializerProto<Node>
{

    public NodeDataSerializerProto(AccessContext serializerCtxRef, ErrorReporter errorReporterRef)
    {
        super(
            serializerCtxRef,
            errorReporterRef,
            InternalApiConsts.API_CHANGED_NODE,
            InternalApiConsts.API_APPLY_NODE
        );
    }

    @Override
    protected void writeData(Node data, ByteArrayOutputStream baos) throws IOException, AccessDeniedException
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("not implemented yet", null);
    }

    @Override
    protected String getName(Node data)
    {
        return data.getName().displayValue;
    }

    @Override
    protected UUID getUuid(Node data)
    {
        return data.getUuid();
    }
}
