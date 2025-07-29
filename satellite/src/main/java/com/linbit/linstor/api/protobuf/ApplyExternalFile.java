package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.ExternalFilePojo;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.c2s.IntExternalFileOuterClass.IntExternalFile;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyExternalFileOuterClass.MsgIntApplyExternalFile;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_EXTERNAL_FILE,
    description = "Registeres an external file on this satellite"
)
@Singleton
public class ApplyExternalFile implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;

    @Inject
    public ApplyExternalFile(
        StltApiCallHandler apiCallHandlerRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntApplyExternalFile msgApplyExternalFile = MsgIntApplyExternalFile.parseDelimitedFrom(msgDataIn);

        ExternalFilePojo extFilePojo = asExternalFilePojo(
            msgApplyExternalFile.getExternalFile(),
            msgApplyExternalFile.getFullSyncId(),
            msgApplyExternalFile.getUpdateId()
        );
        apiCallHandler.applyExternalFileChanges(extFilePojo);
    }

    static ExternalFilePojo asExternalFilePojo(IntExternalFile proto, long fullSyncId, long updateId)
    {
        return new ExternalFilePojo(
            ProtoUuidUtils.deserialize(proto.getUuid()),
            proto.getName(),
            proto.getFlags(),
            proto.getContent().toByteArray(),
            proto.getContentChecksum().toByteArray(),
            fullSyncId,
            updateId
        );
    }
}
