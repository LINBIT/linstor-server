package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.ExternalFilePojo;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntExternalFileDeletedDataOuterClass.MsgIntExternalFileDeletedData;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_DELETED_EXTERNAL_FILE,
    description = "Response to a requested but already deleted external file"
)
@Singleton
public class ApplyDeletedExternalFile implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;

    @Inject
    public ApplyDeletedExternalFile(
        StltApiCallHandler apiCallHandlerRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntExternalFileDeletedData msgApplyDeletedExternalFile = MsgIntExternalFileDeletedData
            .parseDelimitedFrom(msgDataIn);

        ExternalFilePojo extFilePojo = asExternalFilePojo(
            msgApplyDeletedExternalFile.getExternalFileName(),
            msgApplyDeletedExternalFile.getFullSyncId(),
            msgApplyDeletedExternalFile.getUpdateId()
        );
        apiCallHandler.applyDeletedExternalFileChanges(extFilePojo);
    }

    static ExternalFilePojo asExternalFilePojo(String extFileName, long fullSyncId, long updateId)
    {
        return new ExternalFilePojo(
            null,
            extFileName,
            0,
            null,
            null,
            fullSyncId,
            updateId
        );
    }
}
