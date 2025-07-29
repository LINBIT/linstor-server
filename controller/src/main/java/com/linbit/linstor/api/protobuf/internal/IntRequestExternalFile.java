package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtoUuidUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.ExternalFileInternalCallHandler;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_EXTERNAL_FILE,
    description = "Called by the satellite to request external file update data",
    transactional = false
)
@Singleton
public class IntRequestExternalFile implements ApiCall
{
    private final ExternalFileInternalCallHandler externalFilesInternalCallHandler;

    @Inject
    public IntRequestExternalFile(ExternalFileInternalCallHandler apiCallHandlerRef)
    {
        externalFilesInternalCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        IntObjectId extFileId = IntObjectId.parseDelimitedFrom(msgDataIn);
        UUID extFileUuid = ProtoUuidUtils.deserialize(extFileId.getUuid());
        String extFileNameStr = extFileId.getName();

        externalFilesInternalCallHandler.handleExternalFileRequest(extFileUuid, extFileNameStr);
    }
}
