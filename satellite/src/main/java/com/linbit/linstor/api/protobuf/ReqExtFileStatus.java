package com.linbit.linstor.api.protobuf;

import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.pojo.ExtFileStatusPojo;
import com.linbit.linstor.core.StltExternalFileHandler;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntReqExtFileStatusOuterClass.MsgIntReqExtFileStatus;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntExtFileStatusOuterClass.MsgIntExtFileStatus;
import com.linbit.linstor.storage.StorageException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_EXT_FILE_STATUS,
    description = "Returns the status of an external file on disk."
)
@Singleton
public class ReqExtFileStatus implements ApiCallReactive
{
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final StltExternalFileHandler stltExtFileHandler;
    private final CommonSerializer commonSerializer;
    private final Provider<Long> apiCallIdProvider;

    @Inject
    public ReqExtFileStatus(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        StltExternalFileHandler stltExtFileHandlerRef,
        CommonSerializer commonSerializerRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdProviderRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        stltExtFileHandler = stltExtFileHandlerRef;
        commonSerializer = commonSerializerRef;
        apiCallIdProvider = apiCallIdProviderRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
    {
        return scopeRunner.fluxInTransactionlessScope(
            "Query external file status",
            lockGuardFactory.buildDeferred(LockType.READ, LockObj.EXT_FILE_MAP),
            () -> executeInScope(msgDataIn)
        );
    }

    private Flux<byte[]> executeInScope(InputStream msgDataIn)
        throws IOException, InvalidNameException, StorageException
    {
        MsgIntReqExtFileStatus reqMsg = MsgIntReqExtFileStatus.parseDelimitedFrom(msgDataIn);
        ExternalFileName extFileName = new ExternalFileName(reqMsg.getExternalFileName());

        ExtFileStatusPojo status = stltExtFileHandler.getStatus(extFileName);

        MsgIntExtFileStatus respMsg = MsgIntExtFileStatus.newBuilder()
            .setActualPath(status.getActualPath())
            .setContentMatch(status.isContentMatch())
            .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        respMsg.writeDelimitedTo(baos);

        return Flux.just(commonSerializer
            .answerBuilder(InternalApiConsts.API_REQUEST_EXT_FILE_STATUS, apiCallIdProvider.get())
            .bytes(baos.toByteArray())
            .build()
        );
    }
}
