package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntBackupShippingFinishedOuterClass.MsgIntBackupShippingFinished;

import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

import com.google.inject.Inject;
import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_FINISHED,
    description = "Called by the controller to indicate that the backup-shipping finished"
)
@Singleton
public class BackupShippingFinished implements ApiCallReactive
{
    private final StltApiCallHandler apiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public BackupShippingFinished(StltApiCallHandler apiCallHandlerRef, ResponseSerializer responseSerializerRef)
    {
        apiCallHandler = apiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataInRef) throws IOException
    {
        MsgIntBackupShippingFinished backupShippingFinished = MsgIntBackupShippingFinished
            .parseDelimitedFrom(msgDataInRef);
        apiCallHandler
            .backupShippingFinished(
                backupShippingFinished.getRscName(),
                backupShippingFinished.getSnapName(),
                backupShippingFinished.getRemoteName()
            );

        return Flux.<ApiCallRc> just(
            ApiCallRcImpl.singleApiCallRc(0 /* internal */, "Backup finished and cleaned up")
        ).transform(responseSerializer::transform);
    }

}
